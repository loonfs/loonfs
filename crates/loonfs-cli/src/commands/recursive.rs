//! Client-side recursive transfers: `put -r`, `get -r`, and `cp -r`.
//!
//! Traversal follows current state unless a snapshot is selected. Each file
//! uses the corresponding single-file operation and commits independently.
//! Transfers use bounded concurrency and report failures per file.

use super::context::{
    create_directory_tolerating_existing, render_target, CommandContext, RemoteDirectoryOutcome,
};
use super::output::{
    CommandData, CommandFailure, CommandOutput, ListingHeadObservation, TreeTransferFailure,
};
use super::tree_failures::TreeTransferFailures;
use crate::args::{CommandKind, RuntimeBehavior};
use crate::error::CliError;
use crate::payload::LocalPayload;
use crate::progress::{ProgressOp, ProgressReporter};
use crate::render::{write_stderr_progress, OutputFormat};
use futures::{stream::FuturesUnordered, Stream, StreamExt};
use loonfs_client::{CommitOptions, CreateDirectoryOptions, NamespacePath, PutFileOptions};
use loonfs_types::AbsolutePath;
use loonfs_types::DestinationBehavior;
use loonfs_types::ErrorCode;
use loonfs_types::PinId;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

mod traversal;
use traversal::{local_tree, remote_tree, TreeEntry};

/// Maximum number of concurrent file operations.
const TREE_TRANSFERS_IN_FLIGHT: usize = 8;

/// One file in a recursive transfer.
struct FileJob<Source, Destination> {
    source: Source,
    destination: Destination,
    /// File length, used for progress totals.
    size_bytes: Option<u64>,
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
    failures: TreeTransferFailures,
    heads: ListingHeadObservation,
}

impl TreeTally {
    fn new() -> Self {
        Self {
            files: 0,
            directories: 0,
            failures: TreeTransferFailures::default(),
            heads: ListingHeadObservation::default(),
        }
    }

    fn record_file(
        &mut self,
        path: String,
        result: Result<String, CliError>,
        runtime: RuntimeBehavior,
        verb: &str,
    ) -> Result<(), CliError> {
        match result {
            Ok(target) => {
                self.files += 1;
                if runtime.progress.human_lines_enabled() {
                    write_stderr_progress(format_args!("{verb} {target}"));
                }
            }
            Err(error) => self.fail(path, error)?,
        }
        Ok(())
    }

    fn record_directory(&mut self, outcome: DirectoryOutcome) {
        if outcome == DirectoryOutcome::Created {
            self.directories += 1;
        }
    }

    fn fail(&mut self, path: impl Into<String>, error: CliError) -> Result<(), CliError> {
        self.failures
            .push(TreeTransferFailure {
                path: path.into(),
                error,
            })
            .map_err(|error| {
                CliError::new(
                    "io_error",
                    format!("could not save recursive failure report: {error}"),
                )
            })
    }
}

fn create_local_directory(path: &Path) -> std::io::Result<DirectoryOutcome> {
    if path.is_dir() {
        return Ok(DirectoryOutcome::AlreadyExists);
    }
    std::fs::create_dir_all(path)?;
    Ok(DirectoryOutcome::Created)
}

/// Runs discovery and transfers together. A full transfer set stops discovery;
/// completed successes are counted and discarded immediately. Directory creation
/// finishes before discovery resumes into its children.
async fn transfer_tree<Source, Destination, S, F, FF, D, DF>(
    entries: S,
    transfer: F,
    create_directory: D,
    progress: Option<&ProgressReporter>,
    runtime: RuntimeBehavior,
    verb: &str,
) -> Result<TreeTally, CliError>
where
    S: Stream<Item = TreeEntry<Source, Destination>>,
    F: Fn(FileJob<Source, Destination>) -> FF,
    FF: Future<Output = (String, Result<String, CliError>)>,
    D: Fn(Destination) -> DF,
    DF: Future<Output = (String, Result<DirectoryOutcome, CliError>)>,
{
    futures::pin_mut!(entries);
    let mut pending = FuturesUnordered::new();
    let mut tally = TreeTally::new();
    let mut discovered_files = 0;
    let mut discovered_bytes = Some(0u64);
    let mut finished_discovery = false;
    let mut discovery_failed = false;
    loop {
        tokio::select! {
            biased;
            Some((path, result)) = pending.next(), if !pending.is_empty() => {
                tally.record_file(path, result, runtime, verb)?;
            }
            entry = entries.next(), if !finished_discovery && pending.len() < TREE_TRANSFERS_IN_FLIGHT => {
                match entry {
                    Some(TreeEntry::File(job)) => {
                        discovered_files += 1;
                        discovered_bytes = discovered_bytes.zip(job.size_bytes)
                            .and_then(|(total, bytes)| total.checked_add(bytes));
                        pending.push(transfer(job));
                    }
                    Some(TreeEntry::Directory(destination)) => {
                        let directory = create_directory(destination);
                        futures::pin_mut!(directory);
                        loop {
                            // A file may hold the embedded writer's lock while
                            // mkdir waits for it. Keep polling transfers here.
                            tokio::select! {
                                biased;
                                Some((path, result)) = pending.next(), if !pending.is_empty() => {
                                    tally.record_file(path, result, runtime, verb)?;
                                }
                                (path, result) = &mut directory => {
                                    match result {
                                        Ok(outcome) => {
                                            tally.record_directory(outcome);
                                            if runtime.progress.human_lines_enabled() {
                                                write_stderr_progress(format_args!("{} {path}", outcome.progress_label()));
                                            }
                                        }
                                        Err(error) => tally.fail(path, error)?,
                                    }
                                    break;
                                }
                            }
                        }
                    }
                    Some(TreeEntry::Head(head)) => tally.heads.observe(head),
                    Some(TreeEntry::Failure(path, error)) => {
                        discovery_failed = true;
                        tally.fail(path, error)?;
                    },
                    None => {
                        finished_discovery = true;
                        if let Some(progress) = progress {
                            if !discovery_failed {
                                progress.expect(discovered_bytes, Some(discovered_files));
                            }
                        }
                    }
                }
            }
            else => break,
        }
    }
    if let Some(progress) = progress {
        progress.finish();
    }
    Ok(tally)
}

async fn create_remote_directory(
    context: &CommandContext,
    remote: AbsolutePath,
    parents: bool,
    message: Option<String>,
) -> (String, Result<DirectoryOutcome, CliError>) {
    let result = async {
        let spec = remote_spec(context, &remote, "destination_path")?;
        create_directory_tolerating_existing(
            context,
            &spec,
            &CreateDirectoryOptions {
                commit: CommitOptions {
                    preconditions: Vec::new(),
                    commit_id: None,
                    message,
                },
                parents,
            },
        )
        .await
        .map(|outcome| match outcome {
            RemoteDirectoryOutcome::Created(_) => DirectoryOutcome::Created,
            RemoteDirectoryOutcome::AlreadyExists { .. } => DirectoryOutcome::AlreadyExists,
        })
    }
    .await;
    (remote.to_string(), result)
}

fn warn_drift(tally: &TreeTally, runtime: RuntimeBehavior) {
    if runtime.output == OutputFormat::Human {
        if let Some(drift) = tally.heads.drift() {
            crate::render::write_listing_drift_warning(&drift);
        }
    }
}

/// Uploads files as they are discovered. Empty leaf directories create their
/// ancestors; nonempty directories are created by file uploads.
pub(crate) async fn run_put_tree(
    kind: CommandKind,
    context: &CommandContext,
    local_root: &Path,
    remote_root: &AbsolutePath,
    behavior: DestinationBehavior,
    message: Option<String>,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let entries = local_tree(local_root, remote_root).map_err(|error| context.fail(kind, error))?;
    let progress = Arc::new(ProgressReporter::new(
        runtime,
        ProgressOp::Put,
        render_target(context.namespace(), remote_root),
    ));
    let transfer = |job: FileJob<PathBuf, AbsolutePath>| {
        let message = message.clone();
        let progress = Arc::clone(&progress);
        let remote = job.destination.to_string();
        async move {
            let spec = match remote_spec(context, &job.destination, "local_path") {
                Ok(spec) => spec,
                Err(error) => return (remote, Err(error)),
            };
            // Use the size found during the directory walk when available.
            // If it was unavailable, try again before uploading this file.
            let size_bytes = match job.size_bytes {
                Some(size_bytes) => size_bytes,
                None => match std::fs::metadata(&job.source) {
                    Ok(metadata) => metadata.len(),
                    Err(error) => return (remote, Err(CliError::io_for_path(&job.source, error))),
                },
            };
            progress.file_started(&remote, Some(size_bytes));
            // Recursive uploads use the same streaming and resume behavior as
            // single-file uploads.
            let payload = LocalPayload::file(&job.source, size_bytes);
            let result = super::fs::put_payload(
                context,
                &spec,
                &payload,
                &PutFileOptions {
                    behavior,
                    commit: CommitOptions {
                        preconditions: Vec::new(),
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
    };
    let tally = transfer_tree(
        futures::stream::iter(entries),
        transfer,
        |remote| create_remote_directory(context, remote, true, message.clone()),
        Some(&progress),
        runtime,
        "stored",
    )
    .await
    .map_err(|error| context.fail(kind, error))?;
    Ok(context.output(
        kind,
        CommandData::TreeTransfer {
            source: local_root.display().to_string(),
            destination: render_target(context.namespace(), remote_root),
            files: tally.files,
            directories: tally.directories,
            head_drift: None,
            failures: tally.failures,
        },
    ))
}

/// Downloads files as discovered, creating each parent before entering it.
pub(crate) async fn run_get_tree(
    kind: CommandKind,
    context: &CommandContext,
    remote_root: &AbsolutePath,
    local_root: &Path,
    behavior: DestinationBehavior,
    runtime: RuntimeBehavior,
    snapshot_id: Option<&PinId>,
) -> Result<CommandOutput, CommandFailure> {
    let root_outcome = create_local_directory(local_root)
        .map_err(|error| context.fail(kind, CliError::io_for_path(local_root, error)))?;
    let entries = remote_tree(
        context,
        remote_root,
        "remote_path",
        snapshot_id,
        local_root.to_path_buf(),
    )
    .await
    .map_err(|error| context.fail(kind, error))?;
    let progress = Arc::new(ProgressReporter::new(
        runtime,
        ProgressOp::Get,
        render_target(context.namespace(), remote_root),
    ));
    let transfer = |job: FileJob<AbsolutePath, PathBuf>| {
        let progress = Arc::clone(&progress);
        let FileJob {
            source,
            destination: local,
            size_bytes,
        } = job;
        let remote = source.to_string();
        async move {
            let spec = match remote_spec(context, &source, "remote_path") {
                Ok(spec) => spec,
                Err(error) => return (remote, Err(error)),
            };
            let (mut download, meta) =
                match super::fs::open_resumable_download(context, &spec, None, snapshot_id, &local)
                    .await
                {
                    Ok(opened) => opened,
                    Err(error) => return (remote, Err(error)),
                };
            progress.file_started(&remote, size_bytes);
            let written = super::fs::stream_download_to_file(
                &mut download,
                &local,
                meta.as_ref(),
                behavior,
                super::fs::DestinationName::Given,
                &progress,
            )
            .await;
            if let Ok(bytes_written) = &written {
                progress.file_finished(&remote, *bytes_written);
            }
            (remote, written.map(|_| local.display().to_string()))
        }
    };
    let mut tally = transfer_tree(
        entries,
        transfer,
        |local| async move {
            let result = create_local_directory(&local)
                .map_err(|error| CliError::io_for_path(&local, error));
            (local.display().to_string(), result)
        },
        Some(&progress),
        runtime,
        "wrote",
    )
    .await
    .map_err(|error| context.fail(kind, error))?;
    tally.record_directory(root_outcome);
    warn_drift(&tally, runtime);
    Ok(context.output(
        kind,
        CommandData::TreeTransfer {
            source: render_target(context.namespace(), remote_root),
            destination: local_root.display().to_string(),
            files: tally.files,
            directories: tally.directories,
            head_drift: tally.heads.drift(),
            failures: tally.failures,
        },
    ))
}

/// Copies a directory tree without downloading file contents to the CLI.
pub(crate) async fn run_copy_tree(
    kind: CommandKind,
    context: &CommandContext,
    source_root: &AbsolutePath,
    destination_root: &AbsolutePath,
    behavior: DestinationBehavior,
    message: Option<String>,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let source = remote_spec(context, source_root, "source_path")
        .map_err(|error| context.fail(kind, error))?;
    let destination = remote_spec(context, destination_root, "destination_path")
        .map_err(|error| context.fail(kind, error))?;
    ensure_destination_outside_source(context, &source, &destination)
        .await
        .map_err(|error| context.fail(kind, error))?;
    // Read the source before creating the destination so a missing source has
    // no remote write side effects.
    let entries = remote_tree(
        context,
        source_root,
        "source_path",
        None,
        destination_root.clone(),
    )
    .await
    .map_err(|error| context.fail(kind, error))?;
    let entries =
        futures::stream::iter([TreeEntry::Directory(destination_root.clone())]).chain(entries);
    let transfer = |job: FileJob<AbsolutePath, AbsolutePath>| {
        let message = message.clone();
        let source = job.source.to_string();
        async move {
            let from = remote_spec(context, &job.source, "source_path");
            let to = remote_spec(context, &job.destination, "destination_path");
            let (from, to) = match (from, to) {
                (Ok(from), Ok(to)) => (from, to),
                (Err(error), _) | (_, Err(error)) => return (source, Err(error)),
            };
            let result = context
                .target
                .client
                .copy_path_with_options(
                    &from,
                    &to,
                    context.actor(),
                    &loonfs_client::CopyOptions {
                        behavior,
                        commit: CommitOptions {
                            preconditions: Vec::new(),
                            commit_id: None,
                            message: message.clone(),
                        },
                        expected_destination_inode_id: None,
                        expected_destination_revision_no: None,
                    },
                )
                .await
                .map(|_| spec_target(&to));
            (source, result.map_err(CliError::from))
        }
    };
    let tally = transfer_tree(
        entries,
        transfer,
        |destination| {
            let parents = destination == *destination_root;
            create_remote_directory(context, destination, parents, message.clone())
        },
        None,
        runtime,
        "copied",
    )
    .await
    .map_err(|error| context.fail(kind, error))?;
    warn_drift(&tally, runtime);
    Ok(context.output(
        kind,
        CommandData::TreeTransfer {
            source: render_target(context.namespace(), source_root),
            destination: render_target(context.namespace(), destination_root),
            files: tally.files,
            directories: tally.directories,
            head_drift: tally.heads.drift(),
            failures: tally.failures,
        },
    ))
}

/// Rejects a copy destination at or below the source by walking the parents
/// of the destination's deepest existing entry. The server resolves both
/// paths, so the CLI never compares names itself.
async fn ensure_destination_outside_source(
    context: &CommandContext,
    source: &NamespacePath,
    destination: &NamespacePath,
) -> Result<(), CliError> {
    let client = &context.target.client;
    let source_inode_id = client.stat(source).await?.inode_id;
    let mut probe = destination.absolute_path().clone();
    let mut entry = loop {
        match client
            .stat(&NamespacePath::new(
                destination.namespace().clone(),
                probe.clone(),
            ))
            .await
        {
            Ok(entry) => break entry,
            Err(error)
                if matches!(
                    error.code(),
                    Some(ErrorCode::PathNotFound | ErrorCode::PathConflict | ErrorCode::Forbidden)
                ) =>
            {
                match probe.parent() {
                    Some(parent) => probe = parent,
                    None => return Ok(()),
                }
            }
            Err(error) => return Err(error.into()),
        }
    };
    loop {
        if entry.inode_id == source_inode_id {
            return Err(CliError::invalid_request(
                "a recursive copy destination must be outside the source tree",
            )
            .with_param("destination_path"));
        }
        let Some(parent_inode_id) = entry.parent_inode_id else {
            return Ok(());
        };
        // An ancestor the subject cannot read is one the source walk cannot
        // list either, so the copy cannot reach its own output through it.
        entry = match client
            .stat_by_inode(destination.namespace(), parent_inode_id)
            .await
        {
            Ok(parent) => parent,
            Err(error)
                if matches!(
                    error.code(),
                    Some(ErrorCode::InodeNotFound | ErrorCode::Forbidden)
                ) =>
            {
                return Ok(())
            }
            Err(error) => return Err(error.into()),
        };
    }
}

/// Scopes a remote path to the command's namespace, refusing one past the
/// request bounds under `param`.
fn remote_spec(
    context: &CommandContext,
    path: &AbsolutePath,
    param: &str,
) -> Result<NamespacePath, CliError> {
    path.validate_request_bounds()
        .map_err(|error| CliError::invalid_request(error.to_string()).with_param(param))?;
    Ok(NamespacePath::new(
        context.namespace().clone(),
        path.clone(),
    ))
}

fn spec_target(spec: &NamespacePath) -> String {
    render_target(spec.namespace(), spec.absolute_path())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::args::RuntimeBehavior;
    use crate::progress::ProgressMode;
    use crate::resolve::ResolvedTarget;
    use loonfs::SharedObjectStore;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::PROVIDER_MULTIPART_PART_BYTES;
    use loonfs_test_support::stores::BufferWatchStore;
    use loonfs_types::NamespaceId;

    /// A file of two transfer parts and a bit: enough that holding it whole
    /// would show up plainly against holding one part of it, and enough that
    /// the end of the payload is discovered rather than computed.
    const LARGE_FILE_BYTES: usize =
        2 * loonfs_types::usize_from_u64(PROVIDER_MULTIPART_PART_BYTES) + 4_096;
    const SMALL_FILE: &[u8] = b"small enough to hold";

    fn remote(path: &str) -> AbsolutePath {
        AbsolutePath::parse(path).expect("remote path")
    }

    fn payload(len: usize) -> Vec<u8> {
        (0..251_u8).cycle().take(len).collect()
    }

    /// A run nobody is watching: the accounting still happens, it is just
    /// not reported, which is what a test wants of it.
    fn unwatched() -> RuntimeBehavior {
        RuntimeBehavior {
            output: OutputFormat::Json,
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
        let target = ResolvedTarget::over_store(
            store,
            Some("put-tree-test"),
            loonfs_objectstore::ConfiguredObjectStoreKind::LocalFs,
            loonfs::InlineContentPolicy {
                inline_content_threshold_bytes: None,
                ..Default::default()
            },
            false,
        )
        .await
        .expect("build embedded target");
        let namespace = NamespaceId::parse("demo").expect("valid namespace id");
        let context = CommandContext {
            profile_name: "default".to_owned(),
            mode: "embedded".to_owned(),
            namespace: Some(namespace.clone()),
            actor_id: Some(loonfs_test_support::test_actor()),
            target,
        };
        context
            .target
            .client
            .create_namespace(&namespace, &loonfs_test_support::test_actor())
            .await
            .expect("create namespace");
        (context, watched)
    }

    #[tokio::test]
    async fn recursive_discovery_waits_for_a_free_transfer_slot() {
        use std::cell::Cell;
        use std::task::Poll;

        let discovered = Cell::new(0usize);
        let started = Cell::new(0usize);
        let completed = Cell::new(0usize);
        let entries = futures::stream::iter((0..100).map(|index| {
            discovered.set(discovered.get() + 1);
            assert!(
                discovered.get() - completed.get() <= TREE_TRANSFERS_IN_FLIGHT,
                "discovery outran the transfer slots"
            );
            TreeEntry::File(FileJob {
                source: PathBuf::new(),
                destination: remote(&format!("/{index}")),
                size_bytes: Some(1),
            })
        }));
        let tally = transfer_tree(
            entries,
            |job| {
                let started = &started;
                let completed = &completed;
                async move {
                    started.set(started.get() + 1);
                    futures::future::poll_fn(|cx| {
                        // Hold the first batch until all slots are occupied. This
                        // tests both backpressure and real concurrent scheduling.
                        if started.get() < TREE_TRANSFERS_IN_FLIGHT {
                            cx.waker().wake_by_ref();
                            return Poll::Pending;
                        }
                        Poll::Ready(())
                    })
                    .await;
                    completed.set(completed.get() + 1);
                    (job.destination.to_string(), Ok(job.destination.to_string()))
                }
            },
            |_| async { panic!("no directories in this stream") },
            None,
            unwatched(),
            "stored",
        )
        .await
        .expect("transfer report");
        assert_eq!(tally.files, 100);
        assert!(tally.failures.is_empty());
        assert_eq!(completed.get(), 100);
    }

    #[tokio::test]
    async fn recursive_directory_creation_keeps_in_flight_files_running() {
        let directory_started = tokio::sync::Notify::new();
        let file_finished = tokio::sync::Notify::new();
        let entries = futures::stream::iter([
            TreeEntry::File(FileJob {
                source: PathBuf::new(),
                destination: remote("/first"),
                size_bytes: Some(1),
            }),
            TreeEntry::Directory(remote("/second")),
        ]);
        let tally = transfer_tree(
            entries,
            |job| {
                let directory_started = &directory_started;
                let file_finished = &file_finished;
                async move {
                    directory_started.notified().await;
                    file_finished.notify_one();
                    (job.destination.to_string(), Ok(job.destination.to_string()))
                }
            },
            |_| async {
                directory_started.notify_one();
                file_finished.notified().await;
                ("second".to_owned(), Ok(DirectoryOutcome::Created))
            },
            None,
            unwatched(),
            "stored",
        )
        .await
        .expect("transfer report");
        assert_eq!(tally.files, 1);
        assert_eq!(tally.directories, 1);
    }

    #[tokio::test]
    async fn recursive_discovery_preserves_successes_and_continues_after_a_listing_failure() {
        let entries = futures::stream::iter([
            TreeEntry::File(FileJob {
                source: PathBuf::new(),
                destination: remote("/first"),
                size_bytes: Some(1),
            }),
            TreeEntry::Failure(
                "broken".to_owned(),
                CliError::invalid_request("cannot list"),
            ),
            TreeEntry::Directory(remote("/sibling")),
            TreeEntry::File(FileJob {
                source: PathBuf::new(),
                destination: remote("/sibling/last"),
                size_bytes: Some(1),
            }),
        ]);
        let parent_created = std::cell::Cell::new(false);
        let tally = transfer_tree(
            entries,
            |job| {
                let parent_created = &parent_created;
                async move {
                    if job.destination == "/sibling/last" {
                        assert!(
                            parent_created.get(),
                            "parent must exist before its file starts"
                        );
                    }
                    (job.destination.to_string(), Ok(job.destination.to_string()))
                }
            },
            |_| {
                parent_created.set(true);
                async { ("sibling".to_owned(), Ok(DirectoryOutcome::Created)) }
            },
            None,
            unwatched(),
            "stored",
        )
        .await
        .expect("transfer report");
        assert_eq!(tally.files, 2);
        assert_eq!(tally.directories, 1);
        assert_eq!(tally.failures.len(), 1);
        assert_eq!(
            tally
                .failures
                .iter()
                .expect("read report")
                .next()
                .expect("failure")
                .expect("decode failure")
                .path,
            "broken"
        );
    }

    #[tokio::test]
    async fn recursive_remote_discovery_resumes_parent_pages_after_a_child_listing_fails() {
        let store_dir = tempfile::tempdir().expect("tempdir");
        let (context, _) = watched_context(store_dir.path()).await;
        let tree = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir(tree.path().join("broken")).expect("empty directory");
        std::fs::create_dir(tree.path().join("good")).expect("good directory");
        std::fs::write(tree.path().join("a-first"), b"first").expect("first file");
        std::fs::write(tree.path().join("good/child"), b"child").expect("nested file");
        for index in 0..130 {
            std::fs::write(tree.path().join(format!("z-{index:03}")), b"file").expect("root file");
        }
        run_put_tree(
            CommandKind::FilesystemPut,
            &context,
            tree.path(),
            &AbsolutePath::parse("/up").expect("remote root"),
            DestinationBehavior::NoReplace,
            None,
            unwatched(),
        )
        .await
        .unwrap_or_else(|failure| panic!("populate tree: {:?}", failure.error));
        // Only the first root page has been read. Removing a child after that
        // must be observed when descent reaches it, and must not lose later
        // siblings or the remaining root pages.
        let up = AbsolutePath::parse("/up").expect("remote root");
        let entries = remote_tree(&context, &up, "remote_path", None, PathBuf::new())
            .await
            .expect("start discovery");
        let broken = remote_spec(
            &context,
            &AbsolutePath::parse("/up/broken").expect("path"),
            "remote_path",
        )
        .expect("path");
        context
            .target
            .client
            .delete_path(&broken, context.actor())
            .await
            .expect("delete child");
        let tally = transfer_tree(
            entries,
            |job| async move { (job.source.to_string(), Ok(job.source.to_string())) },
            |local| async move {
                (
                    local.display().to_string(),
                    Ok(DirectoryOutcome::AlreadyExists),
                )
            },
            None,
            unwatched(),
            "wrote",
        )
        .await
        .expect("transfer report");
        assert_eq!(
            tally.files, 132,
            "every root page and the surviving child was visited"
        );
        assert_eq!(tally.failures.len(), 1);
        assert_eq!(
            tally
                .failures
                .iter()
                .expect("read report")
                .next()
                .expect("failure")
                .expect("decode failure")
                .path,
            "/up/broken"
        );
        assert!(tally.heads.drift().is_some());
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
            &AbsolutePath::parse("/up").expect("remote root"),
            DestinationBehavior::NoReplace,
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
        let mut download = context
            .target
            .open_file_download(&spec, None, None, 0)
            .await
            .expect("open uploaded file");
        let mut read = Vec::new();
        while let Some(chunk) = download.next_chunk().await.expect("read uploaded chunk") {
            read.extend_from_slice(&chunk);
        }
        assert_eq!(
            read, large,
            "the file that landed is the file that was walked"
        );
    }
}
