//! Client-side recursive transfers: `put -r`, `get -r`, and `cp -r`.
//!
//! Traversal follows current state unless a snapshot is selected. Each file
//! uses the corresponding single-file operation and commits independently.
//! Transfers use bounded concurrency and report failures per file.

use super::context::{
    create_directory_tolerating_existing, CommandContext, RemoteDirectoryOutcome,
};
use super::output::{
    CommandData, CommandFailure, CommandOutput, ListingHeadDrift, ListingHeadObservation,
    TreeTransferFailure,
};
use crate::args::{CommandKind, RuntimeBehavior};
use crate::error::CliError;
use crate::payload::LocalPayload;
use crate::progress::{ProgressOp, ProgressReporter};
use crate::render::write_stderr_progress;
use futures::StreamExt;
use loonfs_api::{CheckpointId, DestinationBehavior, PathEntryKind};
use loonfs_client::{CommitOptions, CreateDirectoryOptions, NamespacePath, PutFileOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Maximum number of concurrent file operations.
const TREE_TRANSFER_CONCURRENCY: usize = 8;

/// One file or directory in a recursive transfer.
struct FileJob {
    local: PathBuf,
    remote: String,
    /// File length, used for transfer selection and progress totals.
    size_bytes: Option<u64>,
    /// Remote content identity used to resume downloads.
    content_ref: Option<loonfs_api::ContentRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DirectoryOutcome {
    Created,
    AlreadyExists,
}

impl DirectoryOutcome {
    fn progress_label(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::AlreadyExists => "already exists",
        }
    }
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

    fn record_directory(&mut self, outcome: DirectoryOutcome) {
        if outcome == DirectoryOutcome::Created {
            self.directories += 1;
        }
    }

    fn fail(&mut self, path: impl Into<String>, error: CliError) {
        self.failures.push(TreeTransferFailure {
            path: path.into(),
            error,
        });
    }
}

fn create_local_directory(path: &Path) -> std::io::Result<DirectoryOutcome> {
    if path.is_dir() {
        return Ok(DirectoryOutcome::AlreadyExists);
    }
    std::fs::create_dir_all(path)?;
    Ok(DirectoryOutcome::Created)
}

/// Returns the total size when every file length is known.
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

/// Uploads a local directory tree. File uploads create their parent
/// directories, so this function creates directories only for empty subtrees.
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

    // Create empty subtrees before uploading files.
    for components in empty_dirs {
        let remote = joined_remote(remote_root, &components);
        let spec = match parse_remote(context, &remote, "local_path") {
            Ok(spec) => spec,
            Err(error) => {
                tally.fail(remote, error);
                continue;
            }
        };
        let outcome = match create_directory_tolerating_existing(
            context,
            &spec,
            &CreateDirectoryOptions {
                commit: CommitOptions {
                    actor: context.actor().clone(),
                    commit_id: None,
                    message: message.clone(),
                },
                parents: true,
            },
        )
        .await
        {
            Ok(RemoteDirectoryOutcome::Created(_)) => DirectoryOutcome::Created,
            Ok(RemoteDirectoryOutcome::AlreadyExists { .. }) => DirectoryOutcome::AlreadyExists,
            Err(error) => {
                tally.fail(remote, error);
                continue;
            }
        };
        tally.record_directory(outcome);
        if runtime.progress.human_lines_enabled() {
            write_stderr_progress(format_args!(
                "{} {}",
                outcome.progress_label(),
                spec_target(&spec)
            ));
        }
    }

    let progress = Arc::new(ProgressReporter::new(
        runtime,
        ProgressOp::Put,
        format!("{}:{}", context.namespace(), remote_root),
    ));
    progress.expect(tree_bytes(&files), Some(files.len() as u64));
    let outcomes = futures::stream::iter(files.into_iter().map(|job| {
        let message = message.clone();
        let progress = Arc::clone(&progress);
        let remote = format!("{}/{}", remote_root.trim_end_matches('/'), job.remote);
        async move {
            let spec = match parse_remote(context, &remote, "local_path") {
                Ok(spec) => spec,
                Err(error) => return (remote, Err(error)),
            };
            // Use the size found during the directory walk when available.
            // If it was unavailable, try again before uploading this file.
            let size_bytes = match job.size_bytes {
                Some(size_bytes) => size_bytes,
                None => match std::fs::metadata(&job.local) {
                    Ok(metadata) => metadata.len(),
                    Err(error) => return (remote, Err(CliError::io_for_path(&job.local, error))),
                },
            };
            progress.file_started(&remote, Some(size_bytes));
            // Recursive uploads use the same streaming and resume behavior as
            // single-file uploads.
            let payload = LocalPayload::file(&job.local, size_bytes);
            let result = super::fs::put_payload(
                context,
                &spec,
                &payload,
                &PutFileOptions {
                    behavior,
                    commit: CommitOptions {
                        actor: context.actor().clone(),
                        commit_id: None,
                        message,
                    },
                    expected_inode_id: None,
                    expected_revision_no: None,
                },
                &progress,
            )
            .await
            .map(|_| spec_target(&spec));
            if result.is_ok() {
                progress.file_finished(&remote, size_bytes);
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
                if runtime.progress.human_lines_enabled() {
                    write_stderr_progress(format_args!("stored {target}"));
                }
            }
            Err(error) => tally.fail(remote, error),
        }
    }

    Ok(context.output(
        kind,
        CommandData::TreeTransfer {
            source: local_root.display().to_string(),
            destination: format!("{}:{}", context.namespace(), remote_root),
            files: tally.files,
            directories: tally.directories,
            head_drift: None,
            failures: tally.failures,
        },
    ))
}

/// Downloads a directory tree. It creates the destination and all remote
/// directories before downloading files.
pub(crate) async fn run_get_tree(
    kind: CommandKind,
    context: &CommandContext,
    remote_root: &str,
    local_root: &Path,
    force: bool,
    runtime: RuntimeBehavior,
    snapshot_id: Option<&CheckpointId>,
) -> Result<CommandOutput, CommandFailure> {
    let mut tally = TreeTally::new();
    // Fail early if the destination cannot be created.
    let root_outcome = create_local_directory(local_root)
        .map_err(|error| context.fail(kind, CliError::io_for_path(local_root, error)))?;
    tally.record_directory(root_outcome);
    let listing = walk_remote_tree(context, kind, "remote_path", remote_root, snapshot_id).await?;
    if !runtime.json {
        if let Some(drift) = listing.head_drift.as_ref() {
            crate::render::write_listing_drift_warning(drift);
        }
    }
    let head_drift = listing.head_drift;

    for components in &listing.directories {
        let local_dir = local_root.join(components.join("/"));
        match create_local_directory(&local_dir) {
            Ok(outcome) => tally.record_directory(outcome),
            Err(error) => tally.fail(
                local_dir.display().to_string(),
                CliError::io_for_path(&local_dir, error),
            ),
        }
    }

    let progress = Arc::new(ProgressReporter::new(
        runtime,
        ProgressOp::Get,
        format!("{}:{}", context.namespace(), remote_root),
    ));
    progress.expect(tree_bytes(&listing.files), Some(listing.files.len() as u64));
    let outcomes = futures::stream::iter(listing.files.into_iter().map(|job| {
        let progress = Arc::clone(&progress);
        let local = local_root.join(job.local);
        async move {
            let spec = match parse_remote(context, &job.remote, "remote_path") {
                Ok(spec) => spec,
                Err(error) => return (job.remote, Err(error)),
            };
            let (mut download, meta) = match super::fs::open_resumable_download(
                context,
                &spec,
                None,
                snapshot_id,
                job.size_bytes,
                &local,
                job.content_ref.as_ref(),
            )
            .await
            {
                Ok(opened) => opened,
                Err(error) => return (job.remote, Err(error)),
            };
            progress.file_started(&job.remote, job.size_bytes);
            let derived_name = false;
            let written = super::fs::stream_download_to_file(
                &mut download,
                &local,
                meta.as_ref(),
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
                if runtime.progress.human_lines_enabled() {
                    write_stderr_progress(format_args!("wrote {local}"));
                }
            }
            Err(error) => tally.fail(remote, error),
        }
    }

    Ok(context.output(
        kind,
        CommandData::TreeTransfer {
            source: format!("{}:{}", context.namespace(), remote_root),
            destination: local_root.display().to_string(),
            files: tally.files,
            directories: tally.directories,
            head_drift,
            failures: tally.failures,
        },
    ))
}

/// Copies a directory tree without downloading file contents to the CLI.
pub(crate) async fn run_copy_tree(
    kind: CommandKind,
    context: &CommandContext,
    source_root: &str,
    destination_root: &str,
    force: bool,
    message: Option<String>,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let listing = walk_remote_tree(context, kind, "source_path", source_root, None).await?;
    if !runtime.json {
        if let Some(drift) = listing.head_drift.as_ref() {
            crate::render::write_listing_drift_warning(drift);
        }
    }
    let head_drift = listing.head_drift;
    let mut tally = TreeTally::new();

    // Create the destination root and then its child directories in order.
    let mut directories = vec![Vec::new()];
    directories.extend(listing.directories);
    for components in &directories {
        let remote = joined_remote(destination_root, components);
        let spec = match parse_remote(context, &remote, "destination_path") {
            Ok(spec) => spec,
            Err(error) => {
                tally.fail(remote, error);
                continue;
            }
        };
        let outcome = match create_directory_tolerating_existing(
            context,
            &spec,
            &CreateDirectoryOptions {
                commit: CommitOptions {
                    actor: context.actor().clone(),
                    commit_id: None,
                    message: message.clone(),
                },
                parents: components.is_empty(),
            },
        )
        .await
        {
            Ok(RemoteDirectoryOutcome::Created(_)) => DirectoryOutcome::Created,
            Ok(RemoteDirectoryOutcome::AlreadyExists { .. }) => DirectoryOutcome::AlreadyExists,
            Err(error) => {
                tally.fail(remote, error);
                continue;
            }
        };
        tally.record_directory(outcome);
        if runtime.progress.human_lines_enabled() {
            write_stderr_progress(format_args!(
                "{} {}",
                outcome.progress_label(),
                spec_target(&spec)
            ));
        }
    }

    let behavior = if force {
        DestinationBehavior::Replace
    } else {
        DestinationBehavior::NoReplace
    };
    let outcomes = futures::stream::iter(listing.files.into_iter().map(|job| {
        let message = message.clone();
        let destination = joined_remote(
            destination_root,
            &job.local
                .components()
                .map(|component| component.as_os_str().to_string_lossy().into_owned())
                .collect::<Vec<_>>(),
        );
        async move {
            let from = parse_remote(context, &job.remote, "source_path");
            let to = parse_remote(context, &destination, "destination_path");
            let (from, to) = match (from, to) {
                (Ok(from), Ok(to)) => (from, to),
                (Err(error), _) | (_, Err(error)) => return (job.remote, Err(error)),
            };
            let result = context
                .target
                .copy_path(
                    &from,
                    &to,
                    &loonfs_client::CopyOptions {
                        behavior,
                        commit: CommitOptions {
                            actor: context.actor().clone(),
                            commit_id: None,
                            message: message.clone(),
                        },
                        expected_destination_inode_id: None,
                        expected_destination_revision_no: None,
                    },
                )
                .await
                .map(|_| spec_target(&to));
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
                if runtime.progress.human_lines_enabled() {
                    write_stderr_progress(format_args!("copied {target}"));
                }
            }
            Err(error) => tally.fail(remote, error),
        }
    }

    Ok(context.output(
        kind,
        CommandData::TreeTransfer {
            source: format!("{}:{}", context.namespace(), source_root),
            destination: format!("{}:{}", context.namespace(), destination_root),
            files: tally.files,
            directories: tally.directories,
            head_drift,
            failures: tally.failures,
        },
    ))
}

fn parse_remote(
    context: &CommandContext,
    path: &str,
    param: &str,
) -> Result<NamespacePath, CliError> {
    NamespacePath::parse(context.namespace().as_str(), path)
        .map_err(|error| crate::error::CliError::from(error).with_invalid_request_param(param))
}

fn spec_target(spec: &NamespacePath) -> String {
    super::context::render_target(spec.namespace(), spec.absolute_path())
}

/// Collects upload jobs and empty directories from a local tree. Symlinks and
/// special files are reported as failures.
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
            CliError::new(
                "io_error",
                format!(
                    "failed to read directory tree `{}`: {error}",
                    root.display()
                ),
            )
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
                // The upload retries metadata lookup if this first lookup
                // fails.
                size_bytes: entry.metadata().ok().map(|metadata| metadata.len()),
                content_ref: None,
            });
        } else {
            tally.fail(
                entry.path().display().to_string(),
                CliError::invalid_request(
                    "only regular files and directories transfer; symlinks and special \
                     files do not",
                )
                .with_param("local_path"),
            );
        }
    }
    // Files create ancestor directories, so only empty subtrees need mkdir.
    let mut candidates: Vec<Vec<String>> = all_dirs
        .into_iter()
        .filter(|dir| {
            !dirs_with_files
                .iter()
                .any(|with_files| with_files.starts_with(dir.as_slice()))
        })
        .collect();
    // Creating the deepest directory also creates its empty parents.
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
    head_drift: Option<ListingHeadDrift>,
}

/// Walks a remote tree breadth-first. Each directory is listed separately, so
/// concurrent changes may appear in the results.
async fn walk_remote_tree(
    context: &CommandContext,
    kind: CommandKind,
    remote_param: &str,
    root: &str,
    snapshot_id: Option<&CheckpointId>,
) -> Result<RemoteTree, CommandFailure> {
    let mut tree = RemoteTree {
        directories: Vec::new(),
        files: Vec::new(),
        head_drift: None,
    };
    let mut heads = ListingHeadObservation::default();
    let mut queue = std::collections::VecDeque::new();
    queue.push_back((root.trim_end_matches('/').to_owned(), Vec::<String>::new()));
    while let Some((remote_dir, components)) = queue.pop_front() {
        let listed = if remote_dir.is_empty() {
            "/"
        } else {
            &remote_dir
        };
        let spec = parse_remote(context, listed, remote_param)
            .map_err(|error| context.fail(kind, error))?;
        let mut cursor = None;
        loop {
            let response = context
                .target
                .list_path_entries_page(&spec, None, cursor.as_deref(), snapshot_id)
                .await
                .map_err(|error| context.fail(kind, error))?;
            heads.observe(response.head_seq);
            cursor = response.next_cursor;
            for entry in response.entries {
                let Some(name) = entry.display_name.as_ref() else {
                    continue;
                };
                let mut child_components = components.clone();
                child_components.push(name.as_str().to_owned());
                match entry.kind {
                    PathEntryKind::Directory {} => {
                        tree.directories.push(child_components.clone());
                        queue.push_back((
                            format!("{remote_dir}/{name}", name = name.as_str()),
                            child_components,
                        ));
                    }
                    PathEntryKind::File {
                        size_bytes,
                        content_ref,
                        ..
                    } => tree.files.push(FileJob {
                        local: PathBuf::from(child_components.join("/")),
                        remote: format!("{remote_dir}/{name}", name = name.as_str()),
                        size_bytes: Some(size_bytes),
                        content_ref: Some(content_ref),
                    }),
                }
            }
            if cursor.is_none() {
                break;
            }
        }
    }
    tree.head_drift = heads.drift();
    Ok(tree)
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use super::*;
    use crate::args::RuntimeBehavior;
    use crate::progress::ProgressMode;
    use crate::resolve::{EmbeddedTarget, ResolvedTarget};
    use loonfs::{SharedObjectStore, TraceStoreKind};
    use loonfs_api::NamespaceId;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::PROVIDER_MULTIPART_PART_BYTES;
    use loonfs_test_support::stores::BufferWatchStore;

    /// A file of two transfer parts and a bit: enough that holding it whole
    /// would show up plainly against holding one part of it, and enough that
    /// the end of the payload is discovered rather than computed.
    const LARGE_FILE_BYTES: usize = 2 * PROVIDER_MULTIPART_PART_BYTES as usize + 4_096;
    const SMALL_FILE: &[u8] = b"small enough to hold";

    fn payload(len: usize) -> Vec<u8> {
        (0..len).map(|offset| (offset % 251) as u8).collect()
    }

    /// A run nobody is watching: the accounting still happens, it is just
    /// not reported, which is what a test wants of it.
    fn unwatched() -> RuntimeBehavior {
        RuntimeBehavior {
            json: true,
            no_input: true,
            interactive: false,
            progress: ProgressMode::Off,
        }
    }

    /// A tree upload against a store that reports every payload buffer it is
    /// handed, and the namespace to upload into.
    async fn watched_context(
        store_dir: &std::path::Path,
    ) -> (CommandContext, Arc<BufferWatchStore<LocalFsStore>>) {
        let watched = Arc::new(BufferWatchStore::watching_content(
            LocalFsStore::new(store_dir).expect("create local-fs store"),
        ));
        let store: SharedObjectStore = watched.clone();
        let target =
            EmbeddedTarget::over_store(store, Some("put-tree-test"), TraceStoreKind::LocalFs)
                .await
                .expect("build embedded target");
        let namespace = NamespaceId::parse("demo").expect("valid namespace id");
        let context = CommandContext {
            profile_name: "default".to_owned(),
            mode: "embedded".to_owned(),
            namespace: Some(namespace.clone()),
            actor: Some(loonfs_test_support::test_actor()),
            target: ResolvedTarget::Embedded(Box::new(target)),
        };
        context
            .target
            .create_namespace(&namespace)
            .await
            .expect("create namespace");
        (context, watched)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_recursive_put_never_holds_a_whole_file() {
        let store_dir = tempfile::tempdir().expect("tempdir");
        let (context, watched) = watched_context(store_dir.path()).await;

        let tree = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir_all(tree.path().join("docs")).expect("create tree dirs");
        let large = payload(LARGE_FILE_BYTES);
        std::fs::write(tree.path().join("docs/big.bin"), &large).expect("write large file");
        std::fs::write(tree.path().join("small.txt"), SMALL_FILE).expect("write small file");

        let output = match run_put_tree(
            CommandKind::FilesystemPut,
            &context,
            tree.path(),
            "/up",
            false,
            None,
            unwatched(),
        )
        .await
        {
            Ok(output) => output,
            Err(failure) => panic!("recursive put failed: {:?}", failure.error),
        };
        let CommandData::TreeTransfer {
            files, failures, ..
        } = output.data
        else {
            panic!("a recursive put reports a tree transfer");
        };
        assert_eq!(files, 2);
        assert!(failures.is_empty(), "{failures:?}");

        // Read the peaks before reading anything back: a download crosses
        // the same boundary and would count as payload too.
        let peaks = watched.peaks();
        assert_eq!(
            peaks.total_bytes,
            (LARGE_FILE_BYTES + SMALL_FILE.len()) as u64,
            "every payload byte crossed the store boundary exactly once"
        );
        assert!(
            peaks.largest_buffer_bytes <= PROVIDER_MULTIPART_PART_BYTES,
            "no single buffer may exceed one part: largest was {}",
            peaks.largest_buffer_bytes
        );
        assert!(
            peaks.peak_live_bytes <= PROVIDER_MULTIPART_PART_BYTES + SMALL_FILE.len() as u64,
            "the tree held {} bytes at once, past one part of its largest file",
            peaks.peak_live_bytes
        );

        let spec = NamespacePath::parse("demo", "/up/docs/big.bin").expect("valid namespace path");
        assert_eq!(
            context
                .target
                .get_file_bytes_with_options(&spec, &loonfs_client::ReadFileOptions::default())
                .await
                .expect("read the uploaded file back"),
            large,
            "the file that landed is the file that was walked"
        );
    }
}
