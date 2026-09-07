//! Client-side recursive transfers: `put -r`, `get -r`, and `cp -r`.
//!
//! Traversal follows current state unless a snapshot is selected. Each file
//! uses the corresponding single-file operation and commits independently.
//! Transfers use bounded concurrency and report failures per file.

use super::context::{
    create_directory_tolerating_existing, CommandContext, RemoteDirectoryOutcome,
};
use super::output::{
    CommandData, CommandFailure, CommandOutput, ListingHeadObservation, TreeTransferFailure,
};
use super::tree_failures::TreeTransferFailures;
use crate::args::{CommandKind, RuntimeBehavior};
use crate::error::CliError;
use crate::payload::LocalPayload;
use crate::progress::{ProgressOp, ProgressReporter};
use crate::render::write_stderr_progress;
use futures::{stream::FuturesUnordered, Stream, StreamExt};
use loonfs_api::{CheckpointId, DestinationBehavior};
use loonfs_client::{CommitOptions, CreateDirectoryOptions, NamespacePath, PutFileOptions};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

mod traversal;
use traversal::{local_tree, remote_tree, TreeEntry};

/// Maximum number of concurrent file operations.
const TREE_TRANSFER_CONCURRENCY: usize = 8;

/// One file in a recursive transfer.
struct FileJob {
    local: PathBuf,
    remote: String,
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

/// Runs discovery and transfers together. A full transfer set stops discovery;
/// completed successes are counted and discarded immediately. Directory creation
/// finishes before discovery resumes into its children.
async fn transfer_tree<S, F, FF, D, DF>(
    entries: S,
    transfer: F,
    create_directory: D,
    progress: Option<&ProgressReporter>,
    runtime: RuntimeBehavior,
    verb: &str,
) -> Result<TreeTally, CliError>
where
    S: Stream<Item = TreeEntry>,
    F: Fn(FileJob) -> FF,
    FF: Future<Output = (String, Result<String, CliError>)>,
    D: Fn(PathBuf) -> DF,
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
            entry = entries.next(), if !finished_discovery && pending.len() < TREE_TRANSFER_CONCURRENCY => {
                match entry {
                    Some(TreeEntry::File(job)) => {
                        discovered_files += 1;
                        discovered_bytes = discovered_bytes.zip(job.size_bytes)
                            .and_then(|(total, bytes)| total.checked_add(bytes));
                        pending.push(transfer(job));
                    }
                    Some(TreeEntry::Directory(relative)) => {
                        let directory = create_directory(relative);
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
    remote: String,
    parents: bool,
    message: Option<String>,
) -> (String, Result<DirectoryOutcome, CliError>) {
    let result = async {
        let spec = parse_remote(context, &remote, "destination_path")?;
        create_directory_tolerating_existing(
            context,
            &spec,
            &CreateDirectoryOptions {
                commit: CommitOptions {
                    actor: context.actor().clone(),
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
    (remote, result)
}

fn relative_remote(root: &str, relative: &Path) -> String {
    joined_remote(
        root,
        &relative
            .components()
            .map(|part| part.as_os_str().to_string_lossy().into_owned())
            .collect::<Vec<_>>(),
    )
}

fn warn_drift(tally: &TreeTally, runtime: RuntimeBehavior) {
    if !runtime.json {
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
    remote_root: &str,
    force: bool,
    message: Option<String>,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let entries = local_tree(local_root, remote_root).map_err(|error| context.fail(kind, error))?;
    let behavior = if force {
        DestinationBehavior::Replace
    } else {
        DestinationBehavior::NoReplace
    };
    let progress = Arc::new(ProgressReporter::new(
        runtime,
        ProgressOp::Put,
        format!("{}:{}", context.namespace(), remote_root),
    ));
    let transfer = |job: FileJob| {
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
    };
    let tally = transfer_tree(
        futures::stream::iter(entries),
        transfer,
        |relative| {
            create_remote_directory(
                context,
                relative_remote(remote_root, &relative),
                true,
                message.clone(),
            )
        },
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
            destination: format!("{}:{}", context.namespace(), remote_root),
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
    remote_root: &str,
    local_root: &Path,
    force: bool,
    runtime: RuntimeBehavior,
    snapshot_id: Option<&CheckpointId>,
) -> Result<CommandOutput, CommandFailure> {
    let root_outcome = create_local_directory(local_root)
        .map_err(|error| context.fail(kind, CliError::io_for_path(local_root, error)))?;
    let entries = remote_tree(context, remote_root, "remote_path", snapshot_id)
        .await
        .map_err(|error| context.fail(kind, error))?;
    let progress = Arc::new(ProgressReporter::new(
        runtime,
        ProgressOp::Get,
        format!("{}:{}", context.namespace(), remote_root),
    ));
    let transfer = |job: FileJob| {
        let progress = Arc::clone(&progress);
        let local = local_root.join(job.local);
        async move {
            let spec = match parse_remote(context, &job.remote, "remote_path") {
                Ok(spec) => spec,
                Err(error) => return (job.remote, Err(error)),
            };
            let (mut download, meta) =
                match super::fs::open_resumable_download(context, &spec, None, snapshot_id, &local)
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
    };
    let mut tally = transfer_tree(
        entries,
        transfer,
        |relative| async move {
            let local = local_root.join(relative);
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
            source: format!("{}:{}", context.namespace(), remote_root),
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
    source_root: &str,
    destination_root: &str,
    force: bool,
    message: Option<String>,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let source = parse_remote(context, source_root, "source_path")
        .map_err(|error| context.fail(kind, error))?;
    let destination = parse_remote(context, destination_root, "destination_path")
        .map_err(|error| context.fail(kind, error))?;
    let source_key = loonfs_api::name_key_for_display_name(source.absolute_path().as_str());
    let source_path = source_key.trim_end_matches('/');
    let destination_key =
        loonfs_api::name_key_for_display_name(destination.absolute_path().as_str());
    let destination_path = destination_key.trim_end_matches('/');
    if destination_path == source_path || destination_path.starts_with(&format!("{source_path}/")) {
        return Err(context.fail(
            kind,
            CliError::invalid_request(
                "a recursive copy destination must be outside the source tree",
            )
            .with_param("destination_path"),
        ));
    }
    // Read the source before creating the destination so a missing source has
    // no remote write side effects.
    let entries = remote_tree(context, source_root, "source_path", None)
        .await
        .map_err(|error| context.fail(kind, error))?;
    let entries = futures::stream::iter([TreeEntry::Directory(PathBuf::new())]).chain(entries);
    let behavior = if force {
        DestinationBehavior::Replace
    } else {
        DestinationBehavior::NoReplace
    };
    let transfer = |job: FileJob| {
        let message = message.clone();
        let destination = relative_remote(destination_root, &job.local);
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
    };
    let tally = transfer_tree(
        entries,
        transfer,
        |relative| {
            create_remote_directory(
                context,
                relative_remote(destination_root, &relative),
                relative.as_os_str().is_empty(),
                message.clone(),
            )
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
            source: format!("{}:{}", context.namespace(), source_root),
            destination: format!("{}:{}", context.namespace(), destination_root),
            files: tally.files,
            directories: tally.directories,
            head_drift: tally.heads.drift(),
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
                discovered.get() - completed.get() <= TREE_TRANSFER_CONCURRENCY,
                "discovery outran the transfer slots"
            );
            TreeEntry::File(FileJob {
                local: PathBuf::new(),
                remote: index.to_string(),
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
                        if started.get() < TREE_TRANSFER_CONCURRENCY {
                            cx.waker().wake_by_ref();
                            return Poll::Pending;
                        }
                        Poll::Ready(())
                    })
                    .await;
                    completed.set(completed.get() + 1);
                    (job.remote.clone(), Ok(job.remote))
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
                local: PathBuf::new(),
                remote: "first".to_owned(),
                size_bytes: Some(1),
            }),
            TreeEntry::Directory(PathBuf::from("second")),
        ]);
        let tally = transfer_tree(
            entries,
            |job| {
                let directory_started = &directory_started;
                let file_finished = &file_finished;
                async move {
                    directory_started.notified().await;
                    file_finished.notify_one();
                    (job.remote.clone(), Ok(job.remote))
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
                local: PathBuf::new(),
                remote: "first".to_owned(),
                size_bytes: Some(1),
            }),
            TreeEntry::Failure(
                "broken".to_owned(),
                CliError::invalid_request("cannot list"),
            ),
            TreeEntry::Directory(PathBuf::from("sibling")),
            TreeEntry::File(FileJob {
                local: PathBuf::new(),
                remote: "sibling/last".to_owned(),
                size_bytes: Some(1),
            }),
        ]);
        let parent_created = std::cell::Cell::new(false);
        let tally = transfer_tree(
            entries,
            |job| {
                let parent_created = &parent_created;
                async move {
                    if job.remote == "sibling/last" {
                        assert!(
                            parent_created.get(),
                            "parent must exist before its file starts"
                        );
                    }
                    (job.remote.clone(), Ok(job.remote))
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
            "/up",
            false,
            None,
            unwatched(),
        )
        .await
        .unwrap_or_else(|failure| panic!("populate tree: {:?}", failure.error));
        // Only the first root page has been read. Removing a child after that
        // must be observed when descent reaches it, and must not lose later
        // siblings or the remaining root pages.
        let entries = remote_tree(&context, "/up", "remote_path", None)
            .await
            .expect("start discovery");
        let broken = parse_remote(&context, "/up/broken", "remote_path").expect("path");
        context
            .target
            .delete_path(
                &broken,
                &loonfs_client::DeleteOptions::new(context.actor().clone()),
            )
            .await
            .expect("delete child");
        let tally = transfer_tree(
            entries,
            |job| async move { (job.remote.clone(), Ok(job.remote)) },
            |relative| async move {
                (
                    relative.display().to_string(),
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
