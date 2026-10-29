//! The `get` command: one file to a local path or to standard output, or a
//! directory tree, with downloads that resume from a partial file.

use super::mutate::destination_behavior;
use crate::args::{FilesystemGetArgs, RuntimeBehavior};
use crate::commands::context::{
    destination_path_for_get, namespace_path, render_target, Invocation, NamespaceContext, RootPath,
};
use crate::commands::download::{FileDownload, FileVersion};
use crate::commands::output::{CommandData, CommandFailure, CommandOutput};
use crate::commands::partial::{self, PartialDownload, PartialDownloadMetadata};
use crate::commands::recursive;
use crate::error::{CliError, Result};
use crate::progress::{ProgressOp, ProgressReporter};
use crate::render::OutputFormat;
use loonfs_client::{NamespacePath, ReadAt};
use loonfs_types::{DestinationBehavior, EntryInodeKind};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(crate) async fn run_filesystem_get(
    invocation: &Invocation,
    args: FilesystemGetArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = NamespaceContext::resolve(invocation, None).await?;
    let behavior = invocation.behavior;
    if behavior.output == OutputFormat::Json && args.local_destination.as_deref() == Some("-") {
        return Err(context.fail(CliError::json_not_supported()));
    }

    let root = if args.recursive {
        RootPath::Allowed
    } else {
        RootPath::Refused
    };
    let spec = namespace_path(context.namespace(), "remote_path", &args.remote_path, root)
        .map_err(|error| context.fail(error))?;
    let entry = context
        .profile
        .target
        .stat_without_attributes_at_snapshot(&spec, args.snapshot_id.as_ref())
        .await
        .map_err(|error| context.fail(error))?;
    if args.recursive {
        return get_tree(&context, &spec, entry.inode_kind(), &args, behavior).await;
    }
    if entry.inode_kind() == EntryInodeKind::Directory {
        return Err(context.fail(
            CliError::invalid_request(format!(
                "`{}` is a directory; use `loonfs get -r` to download the tree",
                spec.absolute_path()
            ))
            .with_param("remote_path"),
        ));
    }

    let version = FileVersion::from_flags(args.revision, args.snapshot_id.clone());
    if args.local_destination.as_deref() == Some("-") {
        return get_to_stdout(&context, &spec, &version).await;
    }
    get_to_file(&context, &spec, &version, &args, behavior).await
}

/// Downloads the directory tree at `spec` with `get -r`.
async fn get_tree(
    context: &NamespaceContext,
    spec: &NamespacePath,
    inode_kind: EntryInodeKind,
    args: &FilesystemGetArgs,
    behavior: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    if inode_kind != EntryInodeKind::Directory {
        return Err(context.fail(
            CliError::invalid_request(format!(
                "`{}` is not a directory; drop -r to download one file",
                spec.absolute_path()
            ))
            .with_param("-r"),
        ));
    }
    if args.revision.is_some() {
        return Err(context.fail(
            CliError::invalid_request("`--revision` applies to one file, not a tree")
                .with_param("--revision"),
        ));
    }
    let local_root = match args.local_destination.as_deref() {
        Some("-") => {
            return Err(context.fail(
                CliError::invalid_request("`-` streams one file; a tree needs a directory")
                    .with_param("local_destination"),
            ))
        }
        Some(destination) => PathBuf::from(destination),
        None => destination_path_for_get(spec.absolute_path(), None)
            .map_err(|error| context.fail(error))?,
    };
    recursive::run_get_tree(
        context,
        spec.absolute_path(),
        &local_root,
        destination_behavior(args.force),
        behavior,
        &ReadAt::from(args.snapshot_id.clone()),
    )
    .await
}

/// Streams one file to standard output for `get <path> -`.
async fn get_to_stdout(
    context: &NamespaceContext,
    spec: &NamespacePath,
    version: &FileVersion,
) -> Result<CommandOutput, CommandFailure> {
    // No progress and no resume: standard output receives the file, and
    // the CLI can't read back bytes that were already piped onward.
    stream_file_to_stdout(context, spec, version)
        .await
        .map_err(|error| context.fail(error))?;
    Ok(context.output(CommandData::StreamedToStdout))
}

/// Downloads one file to a local path, resuming from a partial file that an
/// earlier `get` left behind.
async fn get_to_file(
    context: &NamespaceContext,
    spec: &NamespacePath,
    version: &FileVersion,
    args: &FilesystemGetArgs,
    behavior: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let name = if args.local_destination.is_none() {
        DestinationName::Derived
    } else {
        DestinationName::Given
    };
    let destination =
        destination_path_for_get(spec.absolute_path(), args.local_destination.as_deref())
            .map_err(|error| context.fail(error))?;
    let ResumableDownload {
        mut download,
        partial,
    } = open_resumable_download(context, spec, version, &destination)
        .await
        .map_err(|error| context.fail(error))?;

    let progress = Arc::new(ProgressReporter::new(
        behavior,
        ProgressOp::Get,
        spec.absolute_path().as_str(),
    ));
    let size_bytes = download
        .resume_identity()
        .map(|identity| identity.content_ref.size_bytes);
    progress.expect(size_bytes, Some(1));
    progress.file_started(spec.absolute_path().as_str(), size_bytes);
    // The local working copy is the one thing this CLI touches that has no
    // revision history behind it, so clobbering it is opt-in.
    // `persist_noclobber` closes the race between checking and installing
    // the completed partial file.
    let written = stream_download_to_file(
        &mut download,
        LocalDestination {
            path: &destination,
            behavior: destination_behavior(args.force),
            name,
        },
        partial.as_ref(),
        &progress,
    )
    .await;
    if let Ok(bytes_written) = &written {
        progress.file_finished(spec.absolute_path().as_str(), *bytes_written);
    }
    progress.finish();
    let bytes_written = written.map_err(|error| context.fail(error))?;
    let data = CommandData::FileTransfer {
        target: render_target(context.namespace(), spec.absolute_path()),
        destination: destination.display().to_string(),
        bytes_written,
    };

    Ok(context.output(data))
}

/// A download and the partial-file metadata that a resumed run has to match.
pub(crate) struct ResumableDownload {
    pub(crate) download: FileDownload,
    pub(crate) partial: Option<PartialDownloadMetadata>,
}

/// Opens a download at the offset recorded in a matching partial file.
pub(crate) async fn open_resumable_download(
    context: &NamespaceContext,
    spec: &NamespacePath,
    version: &FileVersion,
    destination: &Path,
) -> Result<ResumableDownload> {
    let mut download = context
        .profile
        .target
        .open_file_download(spec, version, 0)
        .await?;
    let identity = download.resume_identity();
    let resolved_revision = identity.map(|identity| identity.revision_no);
    let meta = identity.map(|identity| {
        PartialDownloadMetadata::describe(identity.content_ref, version.revision_no())
    });
    let start_offset = meta
        .as_ref()
        .map_or(0, |meta| partial::resumable_bytes(destination, meta));
    if start_offset > 0 {
        // Pin the content revision resolved by the first stream. A snapshot
        // already pins its revision and cannot also take a revision selector.
        let resumed = match (version, resolved_revision) {
            (FileVersion::At(ReadAt::Snapshot(_)), _) | (_, None) => version.clone(),
            (_, Some(revision_no)) => FileVersion::Revision(revision_no),
        };
        drop(download);
        download = context
            .profile
            .target
            .open_file_download(spec, &resumed, start_offset)
            .await?;
    }
    Ok(ResumableDownload {
        download,
        partial: meta,
    })
}

/// Writes a download into its destination through the partial file, and
/// installs it only once the download has ended cleanly.
///
/// The partial file is deleted on any failure, including a streamed download
/// whose content failed verification at its last chunk. An integrity failure
/// therefore leaves no file at the destination at all, never a
/// complete-looking one, and nothing beside it for a rerun to build on.
///
/// A download that never reported anything — a killed process — is the one
/// that leaves its partial and its note behind, and the one a rerun picks
/// up. Whatever it picks up is folded into the same verification the whole
/// file gets, so bytes that turn out not to be the file's fail the rerun
/// rather than reaching the destination.
pub(crate) async fn stream_download_to_file(
    download: &mut FileDownload,
    destination: LocalDestination<'_>,
    meta: Option<&PartialDownloadMetadata>,
    progress: &ProgressReporter,
) -> Result<u64> {
    let resumed_from = download.resumed_from();
    let mut partial = PartialDownload::open(destination.path, meta, resumed_from)
        .map_err(|error| destination.open_error(error))?;
    partial.fold_into(download, |error| destination.write_error(error))?;
    progress.already_done(resumed_from);
    if resumed_from > 0 {
        // Folding a head start back into the verification takes time and
        // moves nothing, and it is the one thing about this run a caller
        // could not otherwise tell: it fetches less than the file.
        progress.phase("resuming");
    }
    let mut bytes_written = resumed_from;
    while let Some(chunk) = download.next_chunk().await? {
        partial
            .write_all(&chunk)
            .map_err(|error| destination.write_error(error))?;
        bytes_written += chunk.len() as u64;
        progress.advance(chunk.len() as u64);
    }
    partial
        .install(destination.path, destination.behavior)
        .map_err(|error| destination.write_error(error))?;
    Ok(bytes_written)
}

/// Whether a local destination's name came from the caller or from the
/// remote path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DestinationName {
    /// The caller named the destination.
    Given,
    /// The name is the remote file's name.
    Derived,
}

/// Downloads one file and writes it to standard output as it arrives.
///
/// Bytes are handed on chunk by chunk, so a streamed download that fails its
/// verification at the end fails after some of the file has already been
/// written. That is what streaming to a pipe means — `cat` behaves the same
/// way — and it is why the exit status, not the output, is what says whether
/// the content was verified.
pub(crate) async fn stream_file_to_stdout(
    context: &NamespaceContext,
    spec: &NamespacePath,
    version: &FileVersion,
) -> Result<()> {
    let mut download = context
        .profile
        .target
        .open_file_download(spec, version, 0)
        .await?;
    while let Some(chunk) = download.next_chunk().await? {
        // Locked per chunk rather than across the fetch: nothing else
        // writes to stdout while a download runs, and a guard kept across an
        // await would pin this future to one thread.
        io::stdout()
            .lock()
            .write_all(&chunk)
            .map_err(CliError::io)?;
    }
    io::stdout().lock().flush().map_err(CliError::io)
}

/// A local file that a download writes, and the facts that shape a failure
/// to write it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LocalDestination<'a> {
    pub(crate) path: &'a Path,
    pub(crate) behavior: DestinationBehavior,
    pub(crate) name: DestinationName,
}

impl LocalDestination<'_> {
    /// Shapes the failure to open a download's partial file.
    ///
    /// A missing directory is the one failure whose underlying error is about
    /// the wrong thing: it names the partial file this CLI picked, which the
    /// caller never asked for and cannot act on. The parent they have to
    /// create is what the message says instead.
    fn open_error(&self, error: io::Error) -> CliError {
        if error.kind() == io::ErrorKind::NotFound {
            let parent_error = io::Error::new(
                error.kind(),
                format!(
                    "parent directory `{}` does not exist",
                    partial::parent_of(self.path).display()
                ),
            );
            return CliError::io_for_path(self.path, parent_error);
        }
        self.write_error(error)
    }

    /// Shapes a local write failure the way `get` has always reported one.
    fn write_error(&self, error: io::Error) -> CliError {
        if self.behavior == DestinationBehavior::NoReplace
            && error.kind() == io::ErrorKind::AlreadyExists
        {
            return CliError::destination_exists(self.path);
        }
        let mut error = CliError::io_for_path(self.path, error);
        if self.name == DestinationName::Derived {
            error.message.push_str(
                "; if the remote name exceeds local filesystem limits, pass an \
                 explicit destination or `-` for stdout",
            );
        }
        error
    }
}
