//! Recursive directory listing: a depth-first walk of one directory's
//! subtree, paged with a cursor that records the walk's stack.

use super::materialized_view::{resolved_visible_child, ListedChild, LoadedMetadataView};
use crate::authorize::ReadAccess;
use crate::error::Result;
use crate::metadata::{visible_child, MetadataViewSession, ResolvedVisiblePath};
use loonfs_objectstore::ObjectStore;
use loonfs_types::{
    AttributeInclusion, DirectoryTreeFrame, DirectoryTreePageCursor, InodeKind, NameKey, Page,
    PageRequest, PathEntry, MAX_PATH_DEPTH,
};

/// Most directories on a recursive listing's stack, from the listed
/// directory down, which bounds the walk's memory and the frames in its
/// cursor.
///
/// The walk returns entries up to this many levels below the listed
/// directory, and returns a directory at that depth without descending into
/// it. A request path has at most `MAX_PATH_DEPTH` components, so the walk
/// reaches that depth only in a subtree that a move made deeper than any
/// request path.
pub(super) const MAX_TREE_LISTING_DEPTH: usize = MAX_PATH_DEPTH;

/// One directory on the walk's stack.
struct TreeFrame {
    directory: ResolvedVisiblePath,
    /// The last name key returned from this directory, on this page or an
    /// earlier one, or `None` before the walk returns any.
    last_name_key: Option<NameKey>,
    /// The children this page scanned and hasn't returned yet, the next one
    /// last, or `None` until this page scans the directory.
    ///
    /// A scan reads one more child than the page has room left for, and
    /// every entry the walk returns below this directory uses some of that
    /// room, so one scan per page is enough for every directory. The stack
    /// keeps at most `MAX_TREE_LISTING_DEPTH` times `limit + 1` of these.
    unreturned: Option<Vec<ListedChild>>,
}

impl TreeFrame {
    fn new(directory: ResolvedVisiblePath, last_name_key: Option<NameKey>) -> Self {
        Self {
            directory,
            last_name_key,
            unreturned: None,
        }
    }
}

impl<S: ObjectStore + ?Sized> LoadedMetadataView<'_, S> {
    /// Lists one page of the subtree below `root`, depth first, with each
    /// directory's children in name-key order and each directory before its
    /// descendants. The caller has established that `root` is a directory
    /// the subject can read, and validated any cursor against `root`.
    ///
    /// A child directory that the subject can't read is returned and not
    /// descended into, because listing it on its own would fail.
    ///
    /// Every directory the page scans is one of its entries, apart from
    /// `root` and the directories the cursor records, so a page scans at
    /// most `limit` directories plus the cursor's depth.
    pub(super) async fn directory_tree_page(
        &self,
        session: &mut MetadataViewSession<'_, '_, S>,
        root: &ResolvedVisiblePath,
        request: PageRequest<DirectoryTreePageCursor>,
        attributes: AttributeInclusion,
        access: &ReadAccess<'_, S>,
    ) -> Result<Page<PathEntry, DirectoryTreePageCursor>> {
        let limit = request.limit.as_usize();
        let mut stack = resume_stack(session, root, request.cursor, access).await?;
        let mut returned = Vec::with_capacity(limit);
        let walk_ended = loop {
            let Some(frame) = stack.last_mut() else {
                break true;
            };
            if frame.unreturned.is_none() {
                // A full page scans on, one child per directory, only to
                // learn whether another entry follows.
                let children = self
                    .select_children(
                        session,
                        frame.directory.inode_id,
                        frame.last_name_key.as_ref(),
                        limit - returned.len(),
                    )
                    .await?;
                let mut children = self
                    .listed_children(session, &frame.directory, children, access)
                    .await?;
                children.reverse();
                frame.unreturned = Some(children);
            }
            let Some(child) = frame.unreturned.as_mut().and_then(Vec::pop) else {
                stack.pop();
                continue;
            };
            if returned.len() == limit {
                break false;
            }
            frame.last_name_key = Some(child.name_key.clone());
            if child.readable
                && child.resolved.inode_kind == InodeKind::Directory
                && stack.len() < MAX_TREE_LISTING_DEPTH
            {
                stack.push(TreeFrame::new(child.resolved.clone(), None));
            }
            returned.push(child);
        };
        let next_cursor = (!walk_ended).then(|| DirectoryTreePageCursor {
            head_seq: self.head.seq,
            pin_id: None,
            frames: stack
                .iter()
                .map(|frame| DirectoryTreeFrame {
                    directory_inode_id: frame.directory.inode_id,
                    last_name_key: frame.last_name_key.clone(),
                })
                .collect(),
        });
        let entries = self
            .build_listed_entries(session, returned, attributes)
            .await?;
        Ok(Page {
            items: entries,
            next_cursor,
        })
    }
}

/// Rebuilds the walk's stack from a validated cursor.
///
/// Each level below the listed directory must still be the directory bound
/// at its parent's last name key, and readable. The first level that isn't
/// (because it was deleted, moved, or replaced, or the subject can no
/// longer read it) ends the stack, so the walk continues in its parent
/// after that name key.
async fn resume_stack<S: ObjectStore + ?Sized>(
    session: &mut MetadataViewSession<'_, '_, S>,
    root: &ResolvedVisiblePath,
    cursor: Option<DirectoryTreePageCursor>,
    access: &ReadAccess<'_, S>,
) -> Result<Vec<TreeFrame>> {
    let Some(cursor) = cursor else {
        return Ok(vec![TreeFrame::new(root.clone(), None)]);
    };
    let mut frames = cursor.frames.into_iter();
    let root_frame = frames
        .next()
        .expect("a validated tree cursor has a frame for the listed directory");
    let mut stack = vec![TreeFrame::new(root.clone(), root_frame.last_name_key)];
    for frame in frames {
        let parent = stack
            .last()
            .expect("the stack starts with the listed directory");
        let name_key = parent
            .last_name_key
            .as_ref()
            .expect("only the last frame of a validated tree cursor omits its name key");
        let Some(binding) = visible_child(session, parent.directory.inode_id, name_key).await?
        else {
            break;
        };
        if binding.child_inode_id != frame.directory_inode_id
            || binding.child_kind != InodeKind::Directory
            || !access.can_read(session, binding.child_inode_id).await?
        {
            break;
        }
        let directory = resolved_visible_child(&parent.directory, &binding);
        stack.push(TreeFrame::new(directory, frame.last_name_key));
    }
    Ok(stack)
}
