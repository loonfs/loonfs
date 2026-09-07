//! Incremental depth-first discovery. Local frames retain one directory handle;
//! remote frames retain at most one small listing page. Neither retains a list
//! of every sibling directory or every file in the tree.

use super::{parse_remote, relative_remote, CommandContext, FileJob};
use crate::error::CliError;
use futures::Stream;
use loonfs_api::{ChangeSeq, CheckpointId, ListPathEntriesResponse, PathEntry, PathEntryKind};
use std::fs::ReadDir;
use std::path::{Path, PathBuf};

const TREE_LIST_PAGE_SIZE: u32 = 64;

pub(super) enum TreeEntry {
    File(FileJob),
    Directory(PathBuf),
    Head(ChangeSeq),
    Failure(String, CliError),
}

struct LocalFrame {
    path: PathBuf,
    entries: ReadDir,
    has_children: bool,
}

pub(super) struct LocalTree {
    root: PathBuf,
    remote_root: String,
    stack: Vec<LocalFrame>,
}

pub(super) fn local_tree(root: &Path, remote_root: &str) -> Result<LocalTree, CliError> {
    loonfs_api::AbsolutePath::parse(remote_root)
        .map_err(|error| CliError::invalid_request(error.to_string()).with_param("remote_path"))?;
    let entries = std::fs::read_dir(root).map_err(|error| CliError::io_for_path(root, error))?;
    Ok(LocalTree {
        root: root.to_owned(),
        remote_root: remote_root.to_owned(),
        stack: vec![LocalFrame {
            path: root.to_owned(),
            entries,
            has_children: false,
        }],
    })
}

impl Iterator for LocalTree {
    type Item = TreeEntry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let frame = self.stack.last_mut()?;
            let entry = match frame.entries.next() {
                Some(Ok(entry)) => entry,
                Some(Err(error)) => {
                    frame.has_children = true;
                    return Some(TreeEntry::Failure(
                        frame.path.display().to_string(),
                        CliError::io_for_path(&frame.path, error),
                    ));
                }
                None => {
                    let frame = self.stack.pop().expect("a frame is open");
                    if !frame.has_children {
                        return Some(TreeEntry::Directory(
                            frame
                                .path
                                .strip_prefix(&self.root)
                                .expect("a descendant of the root")
                                .to_owned(),
                        ));
                    }
                    continue;
                }
            };
            let path = entry.path();
            let failure = |error| TreeEntry::Failure(path.display().to_string(), error);
            // Prune paths that cannot exist remotely before opening another
            // directory. This also bounds handles by the API's path depth.
            let relative = path
                .strip_prefix(&self.root)
                .expect("a descendant of the root");
            if let Err(error) =
                loonfs_api::AbsolutePath::parse(relative_remote(&self.remote_root, relative))
            {
                frame.has_children = true;
                return Some(failure(
                    CliError::invalid_request(error.to_string()).with_param("local_path"),
                ));
            }
            let file_type = match entry.file_type() {
                Ok(file_type) => file_type,
                Err(error) => {
                    frame.has_children = true;
                    return Some(failure(CliError::io_for_path(&path, error)));
                }
            };
            if file_type.is_dir() {
                frame.has_children = true;
                match std::fs::read_dir(&path) {
                    Ok(entries) => self.stack.push(LocalFrame {
                        path,
                        entries,
                        has_children: false,
                    }),
                    Err(error) => return Some(failure(CliError::io_for_path(&path, error))),
                }
            } else if file_type.is_file() {
                frame.has_children = true;
                let relative = path
                    .strip_prefix(&self.root)
                    .expect("a descendant of the root");
                let remote = relative
                    .components()
                    .map(|part| part.as_os_str().to_string_lossy())
                    .collect::<Vec<_>>()
                    .join("/");
                return Some(TreeEntry::File(FileJob {
                    remote,
                    size_bytes: entry.metadata().ok().map(|metadata| metadata.len()),
                    local: path,
                }));
            } else {
                return Some(failure(CliError::invalid_request("only regular files and directories transfer; symlinks and special files do not").with_param("local_path")));
            }
        }
    }
}

struct RemoteFrame {
    relative: PathBuf,
    entries: std::vec::IntoIter<PathEntry>,
    cursor: Option<String>,
    needs_page: bool,
}

impl RemoteFrame {
    fn new(relative: PathBuf) -> Self {
        Self {
            relative,
            entries: Vec::new().into_iter(),
            cursor: None,
            needs_page: true,
        }
    }

    fn set_page(&mut self, page: ListPathEntriesResponse) -> ChangeSeq {
        self.entries = page.entries.into_iter();
        self.cursor = page.next_cursor;
        self.needs_page = self.cursor.is_some();
        page.head_seq
    }
}

struct RemoteTree<'a> {
    context: &'a CommandContext,
    root: &'a str,
    param: &'a str,
    snapshot_id: Option<&'a CheckpointId>,
    stack: Vec<RemoteFrame>,
    first_head: Option<ChangeSeq>,
}

pub(super) async fn remote_tree<'a>(
    context: &'a CommandContext,
    root: &'a str,
    param: &'a str,
    snapshot_id: Option<&'a CheckpointId>,
) -> Result<impl Stream<Item = TreeEntry> + 'a, CliError> {
    let spec = parse_remote(context, root, param)?;
    let page = context
        .target
        .list_path_entries_page(&spec, Some(TREE_LIST_PAGE_SIZE), None, snapshot_id)
        .await?;
    let mut frame = RemoteFrame::new(PathBuf::new());
    let first_head = Some(frame.set_page(page));
    let state = RemoteTree {
        context,
        root,
        param,
        snapshot_id,
        stack: vec![frame],
        first_head,
    };
    Ok(futures::stream::unfold(state, |mut state| async move {
        let entry = state.next().await?;
        Some((entry, state))
    }))
}

impl RemoteTree<'_> {
    async fn next(&mut self) -> Option<TreeEntry> {
        if let Some(head) = self.first_head.take() {
            return Some(TreeEntry::Head(head));
        }
        loop {
            let frame = self.stack.last_mut()?;
            if let Some(entry) = frame.entries.next() {
                let Some(name) = entry.display_name else {
                    continue;
                };
                let relative = frame.relative.join(name.as_str());
                match entry.kind {
                    PathEntryKind::Directory {} => {
                        self.stack.push(RemoteFrame::new(relative.clone()));
                        return Some(TreeEntry::Directory(relative));
                    }
                    PathEntryKind::File { size_bytes, .. } => {
                        return Some(TreeEntry::File(FileJob {
                            remote: relative_remote(self.root, &relative),
                            local: relative,
                            size_bytes: Some(size_bytes),
                        }))
                    }
                }
            }
            if frame.needs_page {
                let remote = relative_remote(self.root, &frame.relative);
                let page = async {
                    let spec = parse_remote(self.context, &remote, self.param)?;
                    self.context
                        .target
                        .list_path_entries_page(
                            &spec,
                            Some(TREE_LIST_PAGE_SIZE),
                            frame.cursor.as_deref(),
                            self.snapshot_id,
                        )
                        .await
                }
                .await;
                match page {
                    Ok(page) => return Some(TreeEntry::Head(frame.set_page(page))),
                    Err(error) => {
                        // Earlier files may have committed already. Keep their
                        // results and continue siblings after a listing failure.
                        self.stack.pop();
                        return Some(TreeEntry::Failure(remote, error));
                    }
                }
            }
            self.stack.pop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_discovery_prunes_unrepresentable_depth_before_opening_children() {
        let root = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir_all(root.path().join("allowed/too-deep/never-visited"))
            .expect("nested tree");
        let remote_root = format!("/{}", vec!["d"; loonfs_api::MAX_PATH_DEPTH - 1].join("/"));
        let mut tree = local_tree(root.path(), &remote_root).expect("walk");
        assert!(matches!(tree.next(), Some(TreeEntry::Failure(_, _))));
        assert_eq!(
            tree.stack.len(),
            2,
            "only root and the allowed child were opened"
        );
        assert!(tree.next().is_none());
    }

    #[test]
    fn local_discovery_creates_only_empty_leaves_and_handles_an_empty_root() {
        let root = tempfile::tempdir().expect("tempdir");
        let mut empty = local_tree(root.path(), "/up").expect("walk");
        assert!(
            matches!(empty.next(), Some(TreeEntry::Directory(path)) if path.as_os_str().is_empty())
        );
        assert!(empty.next().is_none());
        std::fs::create_dir_all(root.path().join("empty/leaf")).expect("empty chain");
        std::fs::create_dir_all(root.path().join("full")).expect("nonempty directory");
        std::fs::write(root.path().join("full/file"), b"body").expect("file");
        let entries: Vec<_> = local_tree(root.path(), "/up").expect("walk").collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries.iter().filter(|entry| matches!(entry, TreeEntry::Directory(path) if path == Path::new("empty/leaf"))).count(), 1);
        assert_eq!(
            entries
                .iter()
                .filter(|entry| matches!(entry, TreeEntry::File(_)))
                .count(),
            1
        );
    }
}
