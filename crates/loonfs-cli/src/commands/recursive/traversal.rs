//! Incremental depth-first discovery. Local frames retain one directory handle;
//! remote frames retain at most one small listing page. Neither retains a list
//! of every sibling directory or every file in the tree.

use super::{remote_spec, CommandContext, FileJob};
use crate::commands::pagination::page_request;
use crate::error::CliError;
use crate::render::write_stderr_warning;
use futures::Stream;
use loonfs_types::{AbsolutePath, DisplayName, PinId};
use loonfs_types::{ChangeSeq, ListPathEntriesResponse, PathEntry, PathEntryKind};
use std::ffi::OsStr;
use std::fs::ReadDir;
use std::path::{Path, PathBuf};

const TREE_LIST_PAGE_SIZE: u32 = 64;

pub(super) enum TreeEntry<Source, Destination> {
    File(FileJob<Source, Destination>),
    /// A directory to create, by its destination path.
    Directory(Destination),
    Head(ChangeSeq),
    Failure(String, CliError),
}

/// A destination path that a remote walk extends by one name per entry.
pub(super) trait TreePath: Clone {
    fn child(&self, name: &DisplayName) -> Self;
}

impl TreePath for AbsolutePath {
    fn child(&self, name: &DisplayName) -> Self {
        self.join(name)
    }
}

impl TreePath for PathBuf {
    fn child(&self, name: &DisplayName) -> Self {
        self.join(name.as_str())
    }
}

struct LocalFrame {
    path: PathBuf,
    remote: AbsolutePath,
    entries: ReadDir,
    has_children: bool,
}

pub(super) struct LocalTree {
    stack: Vec<LocalFrame>,
}

pub(super) fn local_tree(root: &Path, remote_root: &AbsolutePath) -> Result<LocalTree, CliError> {
    let entries = std::fs::read_dir(root).map_err(|error| CliError::io_for_path(root, error))?;
    Ok(LocalTree {
        stack: vec![LocalFrame {
            path: root.to_owned(),
            remote: remote_root.clone(),
            entries,
            has_children: false,
        }],
    })
}

/// Reads a local file name as the display name it is stored under.
fn local_display_name(name: &OsStr) -> Result<DisplayName, CliError> {
    DisplayName::parse(crate::commands::context::utf8_local_name(name)?)
        .map_err(|error| CliError::invalid_request(error.to_string()).with_param("local_path"))
}

impl Iterator for LocalTree {
    type Item = TreeEntry<PathBuf, AbsolutePath>;

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
                    let frame = self.stack.pop().expect("a directory frame should be open");
                    if !frame.has_children {
                        return Some(TreeEntry::Directory(frame.remote));
                    }
                    continue;
                }
            };
            let path = entry.path();
            let failure = |error| TreeEntry::Failure(path.display().to_string(), error);
            // Prune paths that cannot exist remotely before opening another
            // directory. This also bounds handles by the API's path depth.
            let remote = local_display_name(&entry.file_name()).and_then(|name| {
                let remote = frame.remote.join(&name);
                remote.validate_request_bounds().map_err(|error| {
                    CliError::invalid_request(error.to_string()).with_param("local_path")
                })?;
                Ok(remote)
            });
            let remote = match remote {
                Ok(remote) => remote,
                Err(error) => {
                    frame.has_children = true;
                    return Some(failure(error));
                }
            };
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
                        remote,
                        entries,
                        has_children: false,
                    }),
                    Err(error) => return Some(failure(CliError::io_for_path(&path, error))),
                }
            } else if file_type.is_file() {
                frame.has_children = true;
                return Some(TreeEntry::File(FileJob {
                    destination: remote,
                    size_bytes: entry.metadata().ok().map(|metadata| metadata.len()),
                    source: path,
                }));
            } else {
                return Some(failure(CliError::invalid_request("only regular files and directories transfer; symlinks and special files do not").with_param("local_path")));
            }
        }
    }
}

struct RemoteFrame<Destination> {
    source: AbsolutePath,
    destination: Destination,
    entries: std::vec::IntoIter<PathEntry>,
    next_page: NextPage,
}

/// Which page of a directory listing a frame reads next.
enum NextPage {
    /// The first page.
    Start,
    /// The page after this cursor.
    After(String),
    /// None: the listing is complete.
    Exhausted,
}

impl<Destination> RemoteFrame<Destination> {
    fn new(source: AbsolutePath, destination: Destination) -> Self {
        Self {
            source,
            destination,
            entries: Vec::new().into_iter(),
            next_page: NextPage::Start,
        }
    }

    fn set_page(&mut self, page: ListPathEntriesResponse) -> ChangeSeq {
        self.entries = page.entries.into_iter();
        self.next_page = page
            .next_cursor
            .map_or(NextPage::Exhausted, NextPage::After);
        page.head_seq
    }
}

struct RemoteTree<'a, Destination> {
    context: &'a CommandContext,
    param: &'a str,
    snapshot_id: Option<&'a PinId>,
    stack: Vec<RemoteFrame<Destination>>,
    first_head: Option<ChangeSeq>,
}

pub(super) async fn remote_tree<'a, Destination: TreePath + 'a>(
    context: &'a CommandContext,
    root: &AbsolutePath,
    param: &'a str,
    snapshot_id: Option<&'a PinId>,
    destination: Destination,
) -> Result<impl Stream<Item = TreeEntry<AbsolutePath, Destination>> + 'a, CliError> {
    let spec = remote_spec(context, root, param)?;
    let page = context
        .target
        .list_at_snapshot(&spec, snapshot_id)
        .page(page_request(None, Some(TREE_LIST_PAGE_SIZE))?)
        .await?;
    let mut frame = RemoteFrame::new(root.clone(), destination);
    let first_head = Some(frame.set_page(page));
    let state = RemoteTree {
        context,
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

impl<Destination: TreePath> RemoteTree<'_, Destination> {
    async fn next(&mut self) -> Option<TreeEntry<AbsolutePath, Destination>> {
        if let Some(head) = self.first_head.take() {
            return Some(TreeEntry::Head(head));
        }
        loop {
            let frame = self.stack.last_mut()?;
            if let Some(entry) = frame.entries.next() {
                let Some(name) = entry.display_name else {
                    continue;
                };
                let source = frame.source.join(&name);
                match entry.kind {
                    PathEntryKind::Directory {} => {
                        let destination = frame.destination.child(&name);
                        self.stack
                            .push(RemoteFrame::new(source, destination.clone()));
                        return Some(TreeEntry::Directory(destination));
                    }
                    PathEntryKind::File { size_bytes, .. } => {
                        return Some(TreeEntry::File(FileJob {
                            destination: frame.destination.child(&name),
                            source,
                            size_bytes: Some(size_bytes),
                        }))
                    }
                    PathEntryKind::Unknown => {
                        write_stderr_warning(format_args!(
                            "skipping `{source}`: this client does not know its inode kind"
                        ));
                        continue;
                    }
                }
            }
            let cursor = match &frame.next_page {
                NextPage::Start => None,
                NextPage::After(cursor) => Some(cursor.clone()),
                NextPage::Exhausted => {
                    self.stack.pop();
                    continue;
                }
            };
            let page = async {
                let spec = remote_spec(self.context, &frame.source, self.param)?;
                Ok(self
                    .context
                    .target
                    .list_at_snapshot(&spec, self.snapshot_id)
                    .page(page_request(cursor, Some(TREE_LIST_PAGE_SIZE))?)
                    .await?)
            }
            .await;
            match page {
                Ok(page) => return Some(TreeEntry::Head(frame.set_page(page))),
                Err(error) => {
                    // Earlier files may have committed already. Keep their
                    // results and continue siblings after a listing failure.
                    let remote = frame.source.to_string();
                    self.stack.pop();
                    return Some(TreeEntry::Failure(remote, error));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn remote_paths_reject_non_utf8_names_without_replacement() {
        use std::os::unix::ffi::OsStrExt;
        for bytes in [b"bad-\xff".as_slice(), b"bad-\xfe".as_slice()] {
            let path = Path::new(std::ffi::OsStr::from_bytes(bytes));
            for result in [
                crate::commands::context::default_remote_put_path(path).map(|_| ()),
                local_display_name(path.as_os_str()).map(|_| ()),
            ] {
                let error = result.expect_err("invalid name");
                assert_eq!(error.code, loonfs_types::ErrorCode::InvalidRequest.as_str());
            }
        }
    }

    #[test]
    fn local_discovery_prunes_unrepresentable_depth_before_opening_children() {
        let root = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir_all(root.path().join("allowed/too-deep/never-visited"))
            .expect("nested tree");
        let remote_root = AbsolutePath::parse(format!(
            "/{}",
            vec!["d"; loonfs_types::MAX_PATH_DEPTH - 1].join("/")
        ))
        .expect("remote root");
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
        let up = AbsolutePath::parse("/up").expect("remote root");
        let mut empty = local_tree(root.path(), &up).expect("walk");
        assert!(matches!(empty.next(), Some(TreeEntry::Directory(path)) if path == up));
        assert!(empty.next().is_none());
        std::fs::create_dir_all(root.path().join("empty/leaf")).expect("empty chain");
        std::fs::create_dir_all(root.path().join("full")).expect("nonempty directory");
        std::fs::write(root.path().join("full/file"), b"body").expect("file");
        let entries: Vec<_> = local_tree(root.path(), &up).expect("walk").collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(
            entries
                .iter()
                .filter(|entry| matches!(entry, TreeEntry::Directory(path) if path.as_str() == "/up/empty/leaf"))
                .count(),
            1
        );
        assert_eq!(
            entries
                .iter()
                .filter(|entry| matches!(entry, TreeEntry::File(_)))
                .count(),
            1
        );
    }
}
