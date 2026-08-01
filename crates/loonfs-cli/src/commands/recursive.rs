//! Client-side recursive transfers: `put -r`, `get -r`, and `cp -r`.
//!
//! Traversal is client-side and unpinned (API spec, section 9.3): remote
//! walks read each directory at its own head through drift-tolerant
//! listings, and every file rides the same per-file operation its singular
//! command uses, as its own independent commit. One process holds one
//! writer session, so concurrent submissions coalesce into batched WAL
//! segments; the transfer runs with bounded concurrency and reports
//! per-file outcomes, so a partial failure reruns per file instead of
//! aborting a giant transaction.

use super::context::CommandContext;
use super::output::{CommandData, CommandFailure, CommandOutput, TreeTransferFailure};
use crate::args::{CommandKind, RuntimeBehavior};
use crate::error::CliError;
use crate::progress::{ProgressOp, ProgressReporter};
use crate::render::write_stderr_progress;
use futures::StreamExt;
use loonfs_api::DestinationBehavior;
use loonfs_api::InodeKind;
use loonfs_client::{CreateDirectoryOptions, NamespacePath, PutFileOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// How many per-file operations run at once. Matches the reference server's
/// default upload concurrency; deliberately a constant, not a flag, until a
/// real workload needs tuning.
const TREE_TRANSFER_CONCURRENCY: usize = 8;

/// One file or directory job, as namespace-absolute remote path plus the
/// local half when one exists.
struct FileJob {
    local: PathBuf,
    remote: String,
    /// The file's length, when the walk that found it stated one. On a
    /// download it also decides how the bytes travel — past the
    /// deployment's proxy cap they come straight from object storage — and
    /// on either transfer it is what lets the whole tree be measured before
    /// the first byte moves.
    size_bytes: Option<u64>,
}

struct TreeTally {
    files: u64,
    directories: u64,
    failures: Vec<TreeTransferFailure>,
}

impl TreeTally {
    fn new() -> Self {
        Self {
            files: 0,
            directories: 0,
            failures: Vec::new(),
        }
    }

    fn fail(&mut self, path: impl Into<String>, error: CliError) {
        self.failures.push(TreeTransferFailure {
            path: path.into(),
            error,
        });
    }
}

/// What the whole tree weighs, or nothing at all when even one file's
/// length is unknown: a total that is missing some files would misreport
/// every percentage derived from it.
fn tree_bytes(files: &[FileJob]) -> Option<u64> {
    files
        .iter()
        .try_fold(0u64, |total, job| Some(total + job.size_bytes?))
}

fn joined_remote(root: &str, components: &[String]) -> String {
    let mut remote = root.trim_end_matches('/').to_owned();
    for component in components {
        remote.push('/');
        remote.push_str(component);
    }
    if remote.is_empty() {
        "/".to_owned()
    } else {
        remote
    }
}

fn output(
    kind: CommandKind,
    context: &CommandContext,
    source: String,
    destination: String,
    tally: TreeTally,
) -> CommandOutput {
    CommandOutput {
        kind,
        profile: Some(context.profile_name.clone()),
        mode: Some(context.mode.clone()),
        data: CommandData::TreeTransfer {
            source,
            destination,
            files: tally.files,
            directories: tally.directories,
            failures: tally.failures,
        },
    }
}

/// Uploads a local directory tree. Ancestor directories materialize through
/// each file's own commit (`parents` semantics), so only subtrees holding no
/// files need explicit `mkdir`s — which also keeps the two paths from racing
/// each other over the same directory.
pub(crate) async fn run_put_tree(
    kind: CommandKind,
    context: &CommandContext,
    local_root: &Path,
    remote_root: &str,
    force: bool,
    message: Option<String>,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let mut files = Vec::new();
    let mut empty_dirs = Vec::new();
    let mut tally = TreeTally::new();
    collect_local_tree(local_root, &mut files, &mut empty_dirs, &mut tally)
        .map_err(|error| context.fail(kind, error))?;

    let behavior = if force {
        DestinationBehavior::Replace
    } else {
        DestinationBehavior::NoReplace
    };

    // Empty subtrees first: nothing else creates them, and their ancestors
    // auto-create exactly like a file's would.
    for components in empty_dirs {
        let remote = joined_remote(remote_root, &components);
        let spec = match NamespacePath::parse(context.namespace.as_str(), &remote) {
            Ok(spec) => spec,
            Err(error) => {
                tally.fail(remote, CliError::invalid_input(error.to_string()));
                continue;
            }
        };
        match context
            .target
            .create_directory(
                &spec,
                &CreateDirectoryOptions {
                    commit_id: None,
                    message: message.clone(),
                    parents: true,
                },
            )
            .await
        {
            Ok(_) => {
                tally.directories += 1;
                if !runtime.json {
                    write_stderr_progress(format_args!("created {}", spec_target(&spec)));
                }
            }
            Err(error) => tally.fail(remote, error.into()),
        }
    }

    let progress = Arc::new(ProgressReporter::new(
        runtime,
        ProgressOp::Put,
        format!("{}:{}", context.namespace, remote_root),
    ));
    progress.expect(tree_bytes(&files), Some(files.len() as u64));
    let outcomes = futures::stream::iter(files.into_iter().map(|job| {
        let backend = &context.target;
        let namespace = context.namespace.clone();
        let message = message.clone();
        let progress = Arc::clone(&progress);
        let remote = format!("{}/{}", remote_root.trim_end_matches('/'), job.remote);
        async move {
            let spec = match NamespacePath::parse(namespace.as_str(), &remote) {
                Ok(spec) => spec,
                Err(error) => return (remote, Err(CliError::invalid_input(error.to_string()))),
            };
            progress.file_started(&remote, job.size_bytes);
            let bytes = match std::fs::read(&job.local) {
                Ok(bytes) => bytes,
                Err(error) => return (remote, Err(CliError::io_for_path(&job.local, error))),
            };
            let result = backend
                .put_file_bytes(
                    &spec,
                    &bytes,
                    &PutFileOptions {
                        behavior,
                        commit_id: None,
                        message: message.clone(),
                        expected_revision_no: None,
                    },
                )
                .await
                .map(|_| spec_target(&spec))
                .map_err(CliError::from);
            // One file of a tree uploads as a single request, so the tree's
            // count advances a whole file at a time: what it reports is
            // files stored, measured in bytes.
            if result.is_ok() {
                progress.advance(bytes.len() as u64);
                progress.file_finished(&remote, bytes.len() as u64);
            }
            (remote, result)
        }
    }))
    .buffer_unordered(TREE_TRANSFER_CONCURRENCY)
    .collect::<Vec<_>>()
    .await;
    progress.finish();
    for (remote, result) in outcomes {
        match result {
            Ok(target) => {
                tally.files += 1;
                if !runtime.json {
                    write_stderr_progress(format_args!("stored {target}"));
                }
            }
            Err(error) => tally.fail(remote, error),
        }
    }

    Ok(output(
        kind,
        context,
        local_root.display().to_string(),
        format!("{}:{}", context.namespace, remote_root),
        tally,
    ))
}

/// Downloads a directory tree. Local directories are created for every
/// remote directory — including empty ones, and including the destination
/// root itself — before any file lands.
pub(crate) async fn run_get_tree(
    kind: CommandKind,
    context: &CommandContext,
    remote_root: &str,
    local_root: &Path,
    force: bool,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let mut tally = TreeTally::new();
    // The destination root, with any missing parents, belongs to this
    // command the way `cp -r`'s target directory belongs to `cp`. It is made
    // before the walk so a destination that can never hold the tree costs
    // one error instead of a full traversal, and its failure ends the
    // command: every file below would fail the same way, and one named
    // error beats one per file. It counts like `cp -r` counts its own
    // destination root, so the same tree reports the same directory total
    // whichever way it is transferred.
    std::fs::create_dir_all(local_root)
        .map_err(|error| context.fail(kind, CliError::io_for_path(local_root, error)))?;
    tally.directories += 1;
    let listing = walk_remote_tree(context, kind, remote_root).await?;

    for components in &listing.directories {
        let local_dir = local_root.join(components.join("/"));
        match std::fs::create_dir_all(&local_dir) {
            Ok(()) => tally.directories += 1,
            Err(error) => tally.fail(
                local_dir.display().to_string(),
                CliError::io_for_path(&local_dir, error),
            ),
        }
    }

    let progress = Arc::new(ProgressReporter::new(
        runtime,
        ProgressOp::Get,
        format!("{}:{}", context.namespace, remote_root),
    ));
    progress.expect(tree_bytes(&listing.files), Some(listing.files.len() as u64));
    let outcomes = futures::stream::iter(listing.files.into_iter().map(|job| {
        let backend = &context.target;
        let namespace = context.namespace.clone();
        let progress = Arc::clone(&progress);
        let local = local_root.join(job.local);
        async move {
            let spec = match NamespacePath::parse(namespace.as_str(), &job.remote) {
                Ok(spec) => spec,
                Err(error) => return (job.remote, Err(CliError::invalid_input(error.to_string()))),
            };
            // One file of a tree travels exactly as a single `get` of it
            // would: past the deployment's proxy cap it streams straight
            // from object storage, and the walk already read the size that
            // decides which.
            let mut download = match backend
                .open_file_download(&spec, None, job.size_bytes)
                .await
            {
                Ok(download) => download,
                Err(error) => return (job.remote, Err(error.into())),
            };
            progress.file_started(&job.remote, job.size_bytes);
            let derived_name = false;
            let written = super::fs::stream_download_to_file(
                &mut download,
                &local,
                force,
                derived_name,
                &progress,
            )
            .await;
            if let Ok(bytes_written) = &written {
                progress.file_finished(&job.remote, *bytes_written);
            }
            (job.remote, written.map(|_| local.display().to_string()))
        }
    }))
    .buffer_unordered(TREE_TRANSFER_CONCURRENCY)
    .collect::<Vec<_>>()
    .await;
    progress.finish();
    for (remote, result) in outcomes {
        match result {
            Ok(local) => {
                tally.files += 1;
                if !runtime.json {
                    write_stderr_progress(format_args!("wrote {local}"));
                }
            }
            Err(error) => tally.fail(remote, error),
        }
    }

    Ok(output(
        kind,
        context,
        format!("{}:{}", context.namespace, remote_root),
        local_root.display().to_string(),
        tally,
    ))
}

/// Copies a directory tree server-side: directories in parent-first order,
/// then files as content-reference copies that move no bytes.
pub(crate) async fn run_copy_tree(
    kind: CommandKind,
    context: &CommandContext,
    source_root: &str,
    destination_root: &str,
    force: bool,
    message: Option<String>,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let listing = walk_remote_tree(context, kind, source_root).await?;
    let mut tally = TreeTally::new();

    // The destination root materializes its own ancestors; every deeper
    // directory arrives in parent-first walk order, so plain `mkdir`
    // suffices and never races a sibling job.
    let mut directories = vec![Vec::new()];
    directories.extend(listing.directories);
    for components in &directories {
        let remote = joined_remote(destination_root, components);
        let spec = match NamespacePath::parse(context.namespace.as_str(), &remote) {
            Ok(spec) => spec,
            Err(error) => {
                tally.fail(remote, CliError::invalid_input(error.to_string()));
                continue;
            }
        };
        match context
            .target
            .create_directory(
                &spec,
                &CreateDirectoryOptions {
                    commit_id: None,
                    message: message.clone(),
                    parents: components.is_empty(),
                },
            )
            .await
        {
            Ok(_) => {
                tally.directories += 1;
                if !runtime.json {
                    write_stderr_progress(format_args!("created {}", spec_target(&spec)));
                }
            }
            Err(error) => tally.fail(remote, error.into()),
        }
    }

    let behavior = if force {
        DestinationBehavior::Replace
    } else {
        DestinationBehavior::NoReplace
    };
    let outcomes = futures::stream::iter(listing.files.into_iter().map(|job| {
        let backend = &context.target;
        let namespace = context.namespace.clone();
        let message = message.clone();
        let destination = joined_remote(
            destination_root,
            &job.local
                .components()
                .map(|component| component.as_os_str().to_string_lossy().into_owned())
                .collect::<Vec<_>>(),
        );
        async move {
            let from = NamespacePath::parse(namespace.as_str(), &job.remote);
            let to = NamespacePath::parse(namespace.as_str(), &destination);
            let (from, to) = match (from, to) {
                (Ok(from), Ok(to)) => (from, to),
                (Err(error), _) | (_, Err(error)) => {
                    return (job.remote, Err(CliError::invalid_input(error.to_string())))
                }
            };
            let result = backend
                .copy_path(
                    &from,
                    &to,
                    &loonfs_client::CopyOptions {
                        behavior,
                        commit_id: None,
                        message: message.clone(),
                    },
                )
                .await
                .map(|_| spec_target(&to))
                .map_err(CliError::from);
            (job.remote, result)
        }
    }))
    .buffer_unordered(TREE_TRANSFER_CONCURRENCY)
    .collect::<Vec<_>>()
    .await;
    for (remote, result) in outcomes {
        match result {
            Ok(target) => {
                tally.files += 1;
                if !runtime.json {
                    write_stderr_progress(format_args!("copied {target}"));
                }
            }
            Err(error) => tally.fail(remote, error),
        }
    }

    Ok(output(
        kind,
        context,
        format!("{}:{}", context.namespace, source_root),
        format!("{}:{}", context.namespace, destination_root),
        tally,
    ))
}

fn spec_target(spec: &NamespacePath) -> String {
    super::context::render_target(spec.namespace(), spec.absolute_path())
}

/// Collects a local tree into file jobs (with relative remote components in
/// `remote`) plus the component paths of subtrees holding no files at all.
/// Entries that are neither files nor directories (symlinks, sockets)
/// become per-path failures rather than silent skips.
fn collect_local_tree(
    root: &Path,
    files: &mut Vec<FileJob>,
    empty_dirs: &mut Vec<Vec<String>>,
    tally: &mut TreeTally,
) -> Result<(), CliError> {
    let mut dirs_with_files = std::collections::BTreeSet::new();
    let mut all_dirs = Vec::new();
    for entry in walkdir::WalkDir::new(root).sort_by_file_name() {
        let entry = entry.map_err(|error| {
            CliError::invalid_input(format!("failed to walk `{}`: {error}", root.display()))
        })?;
        let relative = entry
            .path()
            .strip_prefix(root)
            .expect("walkdir yields paths under its root");
        if relative.as_os_str().is_empty() {
            continue;
        }
        let components: Vec<String> = relative
            .components()
            .map(|component| component.as_os_str().to_string_lossy().into_owned())
            .collect();
        if entry.file_type().is_dir() {
            all_dirs.push(components);
        } else if entry.file_type().is_file() {
            for depth in 1..components.len() {
                dirs_with_files.insert(components[..depth].to_vec());
            }
            files.push(FileJob {
                local: entry.path().to_path_buf(),
                remote: components.join("/"),
                // A local length nobody can read leaves the tree's total
                // unknown rather than wrong, and the upload proceeds either
                // way — the failure that matters surfaces when the file is
                // read.
                size_bytes: entry.metadata().ok().map(|metadata| metadata.len()),
            });
        } else {
            tally.fail(
                entry.path().display().to_string(),
                CliError::invalid_input(
                    "only regular files and directories transfer; symlinks and special \
                     files do not",
                ),
            );
        }
    }
    // A directory is worth an explicit mkdir only when no descendant file
    // will create it — and its ancestors come along through `parents`.
    let mut candidates: Vec<Vec<String>> = all_dirs
        .into_iter()
        .filter(|dir| {
            !dirs_with_files
                .iter()
                .any(|with_files| with_files.starts_with(dir.as_slice()))
        })
        .collect();
    // Keep only the deepest directory of each empty chain: its `parents`
    // mkdir materializes the ancestors, and sequential creation means no
    // sibling job races a shared parent.
    candidates.sort();
    let deepest: Vec<Vec<String>> = candidates
        .iter()
        .filter(|dir| {
            !candidates
                .iter()
                .any(|other| other.len() > dir.len() && other.starts_with(dir.as_slice()))
        })
        .cloned()
        .collect();
    empty_dirs.extend(deepest);
    Ok(())
}

struct RemoteTree {
    /// Relative component paths of every directory, parent-first.
    directories: Vec<Vec<String>>,
    /// File jobs: `local` holds the relative path, `remote` the absolute
    /// namespace path.
    files: Vec<FileJob>,
}

/// Breadth-first remote walk from `root`. Each directory lists at its own
/// head (drift-tolerant listings), so the walk is not a snapshot — the
/// same contract as section 9.3's client-side traversal.
async fn walk_remote_tree(
    context: &CommandContext,
    kind: CommandKind,
    root: &str,
) -> Result<RemoteTree, CommandFailure> {
    let mut tree = RemoteTree {
        directories: Vec::new(),
        files: Vec::new(),
    };
    let mut queue = std::collections::VecDeque::new();
    queue.push_back((root.trim_end_matches('/').to_owned(), Vec::<String>::new()));
    while let Some((remote_dir, components)) = queue.pop_front() {
        let listed = if remote_dir.is_empty() {
            "/"
        } else {
            &remote_dir
        };
        let spec = NamespacePath::parse(context.namespace.as_str(), listed)
            .map_err(|error| context.fail(kind, CliError::invalid_input(error.to_string())))?;
        let entries = context
            .target
            .list_path_entries_all(&spec)
            .await
            .map_err(|error| context.fail(kind, error))?;
        for entry in entries {
            let Some(name) = entry.display_name.as_ref() else {
                continue;
            };
            let mut child_components = components.clone();
            child_components.push(name.as_str().to_owned());
            match entry.inode_kind {
                InodeKind::Directory => {
                    tree.directories.push(child_components.clone());
                    queue.push_back((format!("{remote_dir}/{name}", name = name.as_str()), {
                        child_components
                    }));
                }
                InodeKind::File => tree.files.push(FileJob {
                    local: PathBuf::from(child_components.join("/")),
                    remote: format!("{remote_dir}/{name}", name = name.as_str()),
                    size_bytes: entry.size_bytes,
                }),
            }
        }
    }
    Ok(tree)
}
