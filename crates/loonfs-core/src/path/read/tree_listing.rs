//! Recursive directory listing: a depth-first walk of one directory's
//! subtree, paged with a cursor that records the walk's stack.

use super::materialized_view::{
    resolved_visible_child, ListedChild, LoadedMetadataView, ScannedChild,
};
use crate::authorize::ReadAccess;
use crate::error::Result;
use crate::metadata::{
    visible_child, MetadataViewSession, ResolvedVisiblePath, DIRECTORY_PAGE_RAW_SCAN_LIMIT,
};
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

/// Most children one scan of a directory reads for a recursive listing,
/// besides the one more that shows whether the directory has more.
///
/// The walk scans a directory again for its next children only after it
/// returns the ones it has, so a page keeps at most this many children and
/// one more for each directory on its stack, whatever the width of the
/// tree. One scan reads at least this many binding rows from the manifest,
/// so a smaller batch would make more scans over the same rows.
const MAX_TREE_SCAN_CHILDREN: usize = DIRECTORY_PAGE_RAW_SCAN_LIMIT;

/// One directory on the walk's stack.
struct TreeFrame {
    directory: ResolvedVisiblePath,
    /// The last name key returned from this directory, on this page or an
    /// earlier one, or `None` before the walk returns any.
    last_name_key: Option<NameKey>,
    /// Children scanned after `last_name_key` and not returned yet, the
    /// next one last, at most `MAX_TREE_SCAN_CHILDREN` and one more.
    unreturned: Vec<ScannedChild>,
    /// Whether the last scan reached the directory's last child, so an empty
    /// `unreturned` means the walk is done with the directory.
    scanned_to_end: bool,
}

impl TreeFrame {
    fn new(directory: ResolvedVisiblePath, last_name_key: Option<NameKey>) -> Self {
        Self {
            directory,
            last_name_key,
            unreturned: Vec::new(),
            scanned_to_end: false,
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
            if frame.unreturned.is_empty() && !frame.scanned_to_end {
                // A full page scans on, one child per directory, only to
                // learn whether another entry follows.
                let wanted = (limit - returned.len()).min(MAX_TREE_SCAN_CHILDREN);
                let children = self
                    .select_children(
                        session,
                        frame.directory.inode_id,
                        frame.last_name_key.as_ref(),
                        wanted,
                    )
                    .await?;
                frame.scanned_to_end = children.len() <= wanted;
                frame.unreturned = self.readable_children(session, children, access).await?;
                frame.unreturned.reverse();
            }
            let Some(scanned) = frame.unreturned.pop() else {
                stack.pop();
                continue;
            };
            if returned.len() == limit {
                break false;
            }
            frame.last_name_key = Some(scanned.child.binding.name_key.clone());
            let child = ListedChild::new(&frame.directory, scanned);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authorize::Authorizer;
    use crate::commit_engine::{publish_namespace_commits_batch, CommitCandidate};
    use crate::metadata::LeafRevisionPreload;
    use crate::path::read::load_current_metadata_view;
    use crate::path::write::{CommitIntent, FilesystemOperation};
    use crate::test_support::ops::create;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_test_support::ids::page_limit;
    use loonfs_types::{AbsolutePath, CommitId, NamespaceId};

    /// Directories on the deep tree's chain, from `/deep` down.
    const DEPTH: usize = 8;
    /// Children of each directory on the chain.
    const WIDTH: usize = 400;

    /// Creates a namespace with a chain of `DEPTH` directories from
    /// `/deep` down, each with `WIDTH` children: the next directory on the
    /// chain (`a`, which sorts first) and empty directories after it.
    async fn namespace_with_deep_wide_tree() -> (tempfile::TempDir, LocalFsStore, NamespaceId) {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let context = crate::test_support::mutation_context("tree-listing-tests", 1);
        create(&store, &namespace_id, &context)
            .await
            .expect("bootstrap");
        let directory = |path: String| FilesystemOperation::CreateDirectory {
            path: AbsolutePath::parse(path).expect("path"),
            parents: false,
        };
        let mut chain = "/deep".to_owned();
        let mut operations = vec![directory(chain.clone())];
        for level in 0..DEPTH {
            let first_sibling = if level + 1 < DEPTH {
                operations.push(directory(format!("{chain}/a")));
                1
            } else {
                0
            };
            operations.extend(
                (first_sibling..WIDTH).map(|index| directory(format!("{chain}/e{index:03}"))),
            );
            chain.push_str("/a");
        }
        let mut intent = CommitIntent::single(
            CommitId::generate(),
            loonfs_test_support::test_actor(),
            None,
            operations.remove(0),
        );
        intent.operations.append(&mut operations);
        publish_namespace_commits_batch(
            &store,
            &namespace_id,
            vec![CommitCandidate::new(intent)],
            &context,
            std::sync::Arc::new(tokio::sync::Semaphore::new(32)),
            crate::test_support::std_timer(),
        )
        .await
        .into_iter()
        .next()
        .expect("one result")
        .expect("commit lands");
        (temp_dir, store, namespace_id)
    }

    /// Checks that a recursive page over a deep and wide tree reads at most
    /// the page's limit in children, plus one scan's worth and a lookahead
    /// for each directory on the stack, whatever the width of the tree.
    #[tokio::test]
    async fn a_page_over_a_deep_wide_tree_reads_children_bounded_by_its_limit_and_depth() {
        let (_temp_dir, store, namespace_id) = namespace_with_deep_wide_tree().await;
        let view = load_current_metadata_view(&store, &namespace_id)
            .await
            .expect("load view");
        let mut session = view.metadata_view().session();
        let root = session
            .resolve_visible_path(
                &AbsolutePath::parse("/deep").expect("path"),
                LeafRevisionPreload::Skip,
            )
            .await
            .expect("resolve root");
        let limit = 400;
        let before = session.counters().list_preload_child_lookups;
        let page = view
            .directory_tree_page(
                &mut session,
                &root,
                PageRequest {
                    limit: page_limit(limit),
                    cursor: None,
                },
                AttributeInclusion::Omit,
                &ReadAccess::live(Authorizer::Unrestricted),
            )
            .await
            .expect("recursive page");
        // The session preloads each scanned child's visibility once, so its
        // count of those lookups is the number of children the page read.
        let read_children = session.counters().list_preload_child_lookups - before;

        assert_eq!(page.items.len(), limit);
        assert!(page.next_cursor.is_some());
        let bound = limit + 1 + DEPTH * (MAX_TREE_SCAN_CHILDREN + 1);
        assert!(
            read_children <= bound as u64,
            "a page of {limit} read {read_children} children, more than {bound}"
        );
    }
}
