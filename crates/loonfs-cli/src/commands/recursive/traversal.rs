//! Incremental depth-first discovery of the trees that recursive transfers walk.
//!
//! A local walk keeps one directory handle for each level it has open. A
//! remote walk reads the tree with one recursive listing, and keeps one page
//! of that listing, plus one page for each directory that the listing returned
//! without the entries below it. Neither walk keeps a list of every sibling
//! directory or every file in the tree.

use super::{remote_spec, CommandContext, FileJob};
use crate::commands::pagination::page_request;
use crate::error::CliError;
use crate::render::write_stderr_warning;
use futures::Stream;
use loonfs_types::{AbsolutePath, DisplayName, InodeId, PinId};
use loonfs_types::{ChangeSeq, ListPathEntriesResponse, PathEntry, PathEntryKind};
use std::ffi::OsStr;
use std::fs::ReadDir;
use std::iter::Peekable;
use std::path::{Path, PathBuf};

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

/// One recursive listing in a remote walk, of the walk's root or of a
/// directory that an enclosing listing returned without the entries below it.
struct RemoteListing<Destination> {
    source: AbsolutePath,
    destination: Destination,
    entries: Peekable<std::vec::IntoIter<PathEntry>>,
    next_cursor: Option<String>,
}

impl<Destination> RemoteListing<Destination> {
    fn new(source: AbsolutePath, destination: Destination, page: ListPathEntriesResponse) -> Self {
        Self {
            source,
            destination,
            entries: page.entries.into_iter().peekable(),
            next_cursor: page.next_cursor,
        }
    }

    fn set_page(&mut self, page: ListPathEntriesResponse) -> ChangeSeq {
        self.entries = page.entries.into_iter().peekable();
        self.next_cursor = page.next_cursor;
        page.head_seq
    }
}

/// Returns the walk's source and destination paths for an entry of a listing
/// of `source`, or `None` when the entry's path isn't below `source`.
///
/// The entry's path starts with the listed directory's stored path, which can
/// differ from the spelling on the command line in a case-insensitive
/// namespace, so only the components below the listed directory are joined to
/// the walk's own paths.
fn entry_paths<Destination: TreePath>(
    source: &AbsolutePath,
    destination: &Destination,
    entry: &PathEntry,
) -> Option<(AbsolutePath, Destination)> {
    let below = entry
        .path
        .components()
        .get(source.components().len()..)
        .filter(|below| !below.is_empty())?;
    let mut source = source.clone();
    let mut destination = destination.clone();
    for component in below {
        let name = component.to_display_name();
        source = source.join(&name);
        destination = destination.child(&name);
    }
    Some((source, destination))
}

/// A directory that a listing returned, kept until the next entry shows
/// whether the listing descended into it.
struct ListedDirectory<Destination> {
    inode_id: InodeId,
    source: AbsolutePath,
    destination: Destination,
}

struct RemoteTree<'a, Destination> {
    context: &'a CommandContext,
    param: &'a str,
    snapshot_id: Option<&'a PinId>,
    /// The root's listing, then a listing of each directory that the listing
    /// before it returned without the entries below it.
    ///
    /// Each listing is of a deeper directory than the one before it, and a
    /// directory deeper than a request path can't be listed, so a walk keeps
    /// at most `MAX_PATH_DEPTH + 1` listings, with one page each.
    listings: Vec<RemoteListing<Destination>>,
    last_directory: Option<ListedDirectory<Destination>>,
    first_head: Option<ChangeSeq>,
}

/// Discovers the tree below `root` with one recursive listing, and lists each
/// directory that the listing returns with nothing below it on its own, which
/// reports an unreadable directory with the error of its own listing and costs
/// one request for each empty directory.
pub(super) async fn remote_tree<'a, Destination: TreePath + 'a>(
    context: &'a CommandContext,
    root: &AbsolutePath,
    param: &'a str,
    snapshot_id: Option<&'a PinId>,
    destination: Destination,
) -> Result<impl Stream<Item = TreeEntry<AbsolutePath, Destination>> + 'a, CliError> {
    let mut state = RemoteTree {
        context,
        param,
        snapshot_id,
        listings: Vec::new(),
        last_directory: None,
        first_head: None,
    };
    let page = state.list_page(root, None).await?;
    state.first_head = Some(page.head_seq);
    state
        .listings
        .push(RemoteListing::new(root.clone(), destination, page));
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
            let listing = self.listings.last_mut()?;
            let Some(entry) = listing.entries.peek() else {
                if let Some(cursor) = listing.next_cursor.take() {
                    let source = listing.source.clone();
                    return Some(self.list_next_page(source, cursor).await);
                }
                if let Some(directory) = self.last_directory.take() {
                    return Some(self.list_directory(directory).await);
                }
                self.listings.pop();
                continue;
            };
            let Some((source, destination)) =
                entry_paths(&listing.source, &listing.destination, entry)
            else {
                let message = format!(
                    "the listing of `{}` returned `{}`, which is outside the listed directory",
                    listing.source, entry.path
                );
                let path = entry.path.to_string();
                listing.entries.next();
                return Some(TreeEntry::Failure(path, CliError::client_error(message)));
            };
            // A directory too long to list by path is reported when the walk
            // tries to list it on its own, so the entries below it are skipped.
            if source
                .parent()
                .is_some_and(|parent| parent.validate_request_bounds().is_err())
            {
                listing.entries.next();
                continue;
            }
            if let Some(directory) = self.last_directory.take() {
                if entry.parent_inode_id != Some(directory.inode_id) {
                    return Some(self.list_directory(directory).await);
                }
            }
            let entry = listing.entries.next().expect("the entry was peeked");
            match entry.kind {
                PathEntryKind::Directory {} => {
                    self.last_directory = Some(ListedDirectory {
                        inode_id: entry.inode_id,
                        source,
                        destination: destination.clone(),
                    });
                    return Some(TreeEntry::Directory(destination));
                }
                PathEntryKind::File { size_bytes, .. } => {
                    return Some(TreeEntry::File(FileJob {
                        destination,
                        source,
                        size_bytes: Some(size_bytes),
                    }))
                }
                PathEntryKind::Unknown => {
                    write_stderr_warning(format_args!(
                        "skipping `{source}`: this client does not know its inode kind"
                    ));
                }
            }
        }
    }

    async fn list_next_page(
        &mut self,
        source: AbsolutePath,
        cursor: String,
    ) -> TreeEntry<AbsolutePath, Destination> {
        match self.list_page(&source, Some(cursor)).await {
            Ok(page) => {
                let listing = self
                    .listings
                    .last_mut()
                    .expect("the listing that returned the cursor should be open");
                TreeEntry::Head(listing.set_page(page))
            }
            // A failed page ends its own listing. Earlier files may have
            // committed already, so the walk keeps their results and continues
            // with the enclosing listing, if there is one.
            Err(error) => TreeEntry::Failure(source.to_string(), error),
        }
    }

    /// Lists a directory on its own after a listing returned it without the
    /// entries below it.
    ///
    /// A listing doesn't descend into a directory that the subject can't
    /// read, so listing that directory on its own returns the error that the
    /// walk reports for it. A directory that has entries after all (because
    /// it's deeper than a listing descends, or because the entries were added
    /// after the page was read) is walked with a listing of its own.
    async fn list_directory(
        &mut self,
        directory: ListedDirectory<Destination>,
    ) -> TreeEntry<AbsolutePath, Destination> {
        match self.list_page(&directory.source, None).await {
            Ok(page) => {
                let head = page.head_seq;
                self.listings.push(RemoteListing::new(
                    directory.source,
                    directory.destination,
                    page,
                ));
                TreeEntry::Head(head)
            }
            Err(error) => TreeEntry::Failure(directory.source.to_string(), error),
        }
    }

    async fn list_page(
        &self,
        source: &AbsolutePath,
        cursor: Option<String>,
    ) -> Result<ListPathEntriesResponse, CliError> {
        let spec = remote_spec(self.context, source, self.param)?;
        Ok(self
            .context
            .target
            .list_tree_at_snapshot(&spec, self.snapshot_id)
            .page(page_request(cursor, None)?)
            .await?)
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
