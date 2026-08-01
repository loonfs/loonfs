//! Filesystem commands: ls, stat, get, put, mkdir, rm, mv, cp, revisions,
//! and grep.

use super::context::{
    default_remote_put_path, destination_path_for_get, destination_user_path, directory_intent,
    fail, namespace_path, parse_user_path, render_target, resolve_command_context, CommandContext,
};
use super::output::{CommandData, CommandFailure, CommandOutput};
use super::recursive;
use crate::args::{
    CommandKind, FilesystemCatArgs, FilesystemGetArgs, FilesystemGrepArgs, FilesystemLsArgs,
    FilesystemMkdirArgs, FilesystemPathArgs, FilesystemPutArgs, FilesystemRestoreArgs,
    FilesystemRevisionsArgs, FilesystemRmArgs, FilesystemTransferArgs, FilesystemUndeleteArgs,
    RuntimeBehavior, TrashArgs,
};
use crate::backend::FileDownload;
use crate::error::CliError;
use crate::payload::{read_whole_file, LocalPayload, STDIN_PATH};
use loonfs_api::{
    CommitId, DeleteDirectoryBehavior, DestinationBehavior, ErrorCode, InodeKind, RevisionNo,
};
use loonfs_client::{CreateDirectoryOptions, DeleteOptions, NamespacePath, PutFileOptions};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

// --- filesystem ---

fn parse_commit_id_arg(commit_id: Option<&str>) -> Result<Option<CommitId>, CliError> {
    commit_id
        .map(|value| {
            CommitId::parse(value)
                .map_err(|error| CliError::invalid_input(format!("invalid --commit-id: {error}")))
        })
        .transpose()
}

/// Opens the same-directory temp file a download is written into.
///
/// Dropping it deletes it, which is what makes a failed or interrupted
/// download leave nothing behind at the target.
fn open_partial_file(destination: &Path) -> std::io::Result<tempfile::NamedTempFile> {
    let file_name = destination
        .file_name()
        .ok_or_else(|| std::io::Error::other("destination has no file name"))?;
    let mut prefix = std::ffi::OsString::from(".");
    prefix.push(file_name);
    prefix.push(".loonfs-partial-");
    tempfile::Builder::new()
        .prefix(&prefix)
        .tempfile_in(partial_file_parent(destination))
}

/// The directory a destination's partial file is created in — its own, and
/// the working directory for a bare file name.
fn partial_file_parent(destination: &Path) -> &Path {
    destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

/// Installs a completed download at its destination with one rename.
///
/// Only a download that finished — and, when it was streamed, verified —
/// reaches this, so the file at the destination is never a truncated or
/// unverified one.
fn install_partial_file(
    temp: tempfile::NamedTempFile,
    destination: &Path,
    force: bool,
) -> std::io::Result<()> {
    let persisted = if force {
        temp.persist(destination)
    } else {
        temp.persist_noclobber(destination)
    };
    persisted.map(|_| ()).map_err(|error| error.error)
}

pub(crate) async fn run_filesystem_ls(
    kind: CommandKind,
    config_path: &Path,
    args: FilesystemLsArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let allow_root = true;
    let spec = namespace_path(
        &context.namespace,
        args.path.as_deref().unwrap_or("/"),
        allow_root,
    )
    .map_err(|error| context.fail(kind, error))?;
    let (entries, next_cursor) = match (args.limit, args.cursor.as_deref()) {
        // Unbounded is still the default: a listing nobody bounded prints
        // the whole directory, as it always has.
        (None, None) => {
            let entries = context
                .target
                .list_path_entries_all(&spec)
                .await
                .map_err(|error| context.fail(kind, error))?;
            (entries, None)
        }
        (limit, cursor) => list_bounded_path_entries(&context, kind, &spec, limit, cursor).await?,
    };
    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::PathEntries {
            entries,
            next_cursor,
        },
    })
}

/// Reads pages until `limit` entries are in hand, and reports the cursor
/// that resumes where it stopped.
///
/// `limit` bounds the total, not one page, so the loop asks for what it
/// still needs — clamped to a page size every deployment accepts, because a
/// caller-supplied page limit above `pagination.max_limit` is rejected
/// rather than clamped by the server. An absent `limit` follows the cursor
/// to the end, which is what `--cursor` alone asks for.
async fn list_bounded_path_entries(
    context: &CommandContext,
    kind: CommandKind,
    spec: &NamespacePath,
    limit: Option<u32>,
    cursor: Option<&str>,
) -> Result<(Vec<loonfs_api::AuthoritativePathEntry>, Option<String>), CommandFailure> {
    let mut entries = Vec::new();
    let mut cursor = cursor.map(ToOwned::to_owned);
    loop {
        let page_limit = limit.map(|limit| {
            let remaining = limit.saturating_sub(entries.len() as u32);
            remaining.min(loonfs_api::DEFAULT_PAGE_LIMIT)
        });
        let page = context
            .target
            .list_path_entries_page(spec, page_limit, cursor.as_deref())
            .await
            .map_err(|error| context.fail(kind, error))?;
        entries.extend(page.entries);
        cursor = page.next_cursor;
        let filled = limit.is_some_and(|limit| entries.len() as u32 >= limit);
        if filled || cursor.is_none() {
            return Ok((entries, cursor));
        }
    }
}

pub(crate) async fn run_filesystem_stat(
    kind: CommandKind,
    config_path: &Path,
    args: FilesystemPathArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let allow_root = true;
    let spec = namespace_path(&context.namespace, &args.path, allow_root)
        .map_err(|error| context.fail(kind, error))?;
    let entry = context
        .target
        .stat_path(&spec)
        .await
        .map_err(|error| context.fail(kind, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::PathEntry(entry),
    })
}

pub(crate) async fn run_filesystem_grep(
    kind: CommandKind,
    config_path: &Path,
    args: FilesystemGrepArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let path_prefix = args
        .path_prefix
        .as_deref()
        .map(|path| parse_user_path(path, true))
        .transpose()
        .map_err(|error| context.fail(kind, error))?;
    let mut request = loonfs_api::GrepRequest {
        pattern: args.pattern.clone(),
        case_insensitive: args.ignore_case,
        path_prefix,
        cursor: None,
        limit: args.limit,
        allow_stale: args.allow_stale,
        allow_scan: args.allow_scan,
    };
    let mut matches = Vec::new();
    let mut tail_scanned = true;
    let mut truncated = false;
    // `--limit` sizes a page and is bounded by the deployment's
    // `query.grep.max_limit`; `--max-matches` bounds the whole command. They
    // are separate because a total cap larger than that per-page maximum
    // would be rejected if it were sent as one.
    let max_matches = args.max_matches.map(|max| max as usize);
    let (namespace_id, head_seq, built_through_seq) = loop {
        let response = context
            .target
            .grep(&context.namespace, &request)
            .await
            .map_err(|error| context.fail(kind, error))?;
        let snapshot = (
            response.namespace_id,
            response.head_seq,
            response.built_through_seq,
        );
        matches.extend(response.matches);
        tail_scanned &= response.tail_scanned;
        if let Some(max_matches) = max_matches {
            if matches.len() >= max_matches {
                // A page can overshoot the cap; the extra matches are real
                // but were not asked for.
                truncated = matches.len() > max_matches || response.next_cursor.is_some();
                matches.truncate(max_matches);
                break snapshot;
            }
        }
        match response.next_cursor {
            Some(cursor) => request.cursor = Some(cursor),
            // The final page's snapshot describes the completed query.
            None => break snapshot,
        }
    };
    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::GrepMatches {
            pattern: args.pattern,
            namespace_id,
            head_seq,
            built_through_seq,
            matches,
            tail_scanned,
            truncated,
        },
    })
}

pub(crate) async fn run_filesystem_cat(
    kind: CommandKind,
    config_path: &Path,
    args: FilesystemCatArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let allow_root = false;
    let spec = namespace_path(&context.namespace, &args.path, allow_root)
        .map_err(|error| context.fail(kind, error))?;
    let revision_no = args.revision.map(RevisionNo);
    let bytes = match revision_no {
        Some(revision_no) => {
            context
                .target
                .get_file_revision_bytes(&spec, revision_no)
                .await
        }
        None => context.target.get_file_bytes(&spec).await,
    }
    .map_err(|error| context.fail(kind, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::StreamBytes(bytes),
    })
}

pub(crate) async fn run_filesystem_get(
    kind: CommandKind,
    config_path: &Path,
    args: FilesystemGetArgs,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    if runtime.json && args.local_destination.as_deref() == Some("-") {
        return Err(fail(
            kind,
            Some(context.profile_name),
            Some(context.mode),
            CliError::json_not_supported_for_streaming(),
        ));
    }

    let allow_root = args.recursive;
    let spec = namespace_path(&context.namespace, &args.remote_path, allow_root)
        .map_err(|error| context.fail(kind, error))?;
    let entry = context
        .target
        .stat_path(&spec)
        .await
        .map_err(|error| context.fail(kind, error))?;
    if args.recursive {
        if entry.inode_kind != InodeKind::Directory {
            return Err(context.fail(
                kind,
                CliError::invalid_input(format!(
                    "`{}` is not a directory; drop -r to download one file",
                    spec.absolute_path()
                )),
            ));
        }
        if args.revision.is_some() {
            return Err(context.fail(
                kind,
                CliError::invalid_input("--revision applies to one file, not a tree"),
            ));
        }
        let local_root = match args.local_destination.as_deref() {
            Some("-") => {
                return Err(context.fail(
                    kind,
                    CliError::invalid_input("`-` streams one file; a tree needs a directory"),
                ))
            }
            Some(destination) => PathBuf::from(destination),
            None => destination_path_for_get(spec.absolute_path().as_str(), None)
                .map_err(|error| context.fail(kind, error))?,
        };
        return recursive::run_get_tree(
            kind,
            &context,
            spec.absolute_path().as_str(),
            &local_root,
            args.force,
            runtime,
        )
        .await;
    }
    if entry.inode_kind == InodeKind::Directory {
        return Err(context.fail(
            kind,
            CliError::invalid_input(format!(
                "`{}` is a directory; use `loonfs get -r` to download the tree",
                spec.absolute_path()
            )),
        ));
    }

    let revision_no = args.revision.map(RevisionNo);
    let mut download = context
        .target
        .open_file_download(&spec, revision_no, entry.size_bytes)
        .await
        .map_err(|error| context.fail(kind, error))?;
    let data = match args.local_destination.as_deref() {
        Some("-") => {
            stream_download_to_stdout(&mut download)
                .await
                .map_err(|error| context.fail(kind, error))?;
            CommandData::StreamedToStdout
        }
        other => {
            let derived_name = other.is_none();
            let destination = destination_path_for_get(spec.absolute_path().as_str(), other)
                .map_err(|error| context.fail(kind, error))?;
            // The local working copy is the one thing this CLI touches
            // that has no revision history behind it, so clobbering it is
            // opt-in. `persist_noclobber` closes the race between checking
            // and installing the completed temporary file.
            let bytes_written =
                stream_download_to_file(&mut download, &destination, args.force, derived_name)
                    .await
                    .map_err(|error| context.fail(kind, error))?;
            CommandData::FileTransfer {
                target: render_target(&context.namespace, spec.absolute_path()),
                destination: destination.display().to_string(),
                bytes_written,
            }
        }
    };

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data,
    })
}

/// Writes a download into its destination through the partial file, and
/// installs it only once the download has ended cleanly.
///
/// The temp file is dropped — and so deleted — on any failure, including a
/// streamed download whose content failed verification at its last chunk. An
/// integrity failure therefore leaves no file at the destination at all,
/// never a complete-looking one.
pub(super) async fn stream_download_to_file(
    download: &mut FileDownload,
    destination: &Path,
    force: bool,
    derived_name: bool,
) -> Result<u64, CliError> {
    let mut temp = open_partial_file(destination)
        .map_err(|error| local_open_error(destination, error, force, derived_name))?;
    let mut bytes_written = 0u64;
    while let Some(chunk) = download.next_chunk().await? {
        temp.write_all(&chunk)
            .map_err(|error| local_destination_error(destination, error, force, derived_name))?;
        bytes_written += chunk.len() as u64;
    }
    temp.flush()
        .map_err(|error| local_destination_error(destination, error, force, derived_name))?;
    install_partial_file(temp, destination, force)
        .map_err(|error| local_destination_error(destination, error, force, derived_name))?;
    Ok(bytes_written)
}

/// Writes a download to standard output as it arrives.
///
/// Bytes are handed on chunk by chunk, so a streamed download that fails its
/// verification at the end fails after some of the file has already been
/// written. That is what streaming to a pipe means — `cat` behaves the same
/// way — and it is why the exit status, not the output, is what says whether
/// the content was verified.
async fn stream_download_to_stdout(download: &mut FileDownload) -> Result<(), CliError> {
    while let Some(chunk) = download.next_chunk().await? {
        // Locked per chunk rather than held across the fetch: nothing else
        // writes to stdout while a download runs, and a guard held across an
        // await would pin this future to one thread.
        io::stdout()
            .lock()
            .write_all(&chunk)
            .map_err(CliError::io)?;
    }
    io::stdout().lock().flush().map_err(CliError::io)
}

/// Shapes the failure to open a download's partial file.
///
/// A missing directory is the one failure whose underlying error is about the
/// wrong thing: it names the temporary file this CLI picked, whose random
/// suffix the caller never asked for and cannot act on. The parent they have
/// to create is what the message says instead.
fn local_open_error(
    destination: &Path,
    error: std::io::Error,
    force: bool,
    derived_name: bool,
) -> CliError {
    if error.kind() == std::io::ErrorKind::NotFound {
        return CliError::new(
            "io_error",
            format!(
                "i/o error for `{}`: parent directory `{}` does not exist",
                destination.display(),
                partial_file_parent(destination).display()
            ),
        );
    }
    local_destination_error(destination, error, force, derived_name)
}

/// Shapes a local write failure the way `get` has always reported one.
fn local_destination_error(
    destination: &Path,
    error: std::io::Error,
    force: bool,
    derived_name: bool,
) -> CliError {
    if !force && error.kind() == std::io::ErrorKind::AlreadyExists {
        return CliError::destination_exists(destination);
    }
    let mut error = CliError::io_for_path(destination, error);
    if derived_name {
        error.message.push_str(
            "; if the remote name exceeds local filesystem limits, pass an \
             explicit destination or `-` for stdout",
        );
    }
    error
}

pub(crate) async fn run_filesystem_trash(
    kind: CommandKind,
    config_path: &Path,
    args: TrashArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let response = context
        .target
        .list_trash(&context.namespace, args.limit, args.cursor.as_deref())
        .await
        .map_err(|error| context.fail(kind, error))?;
    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::Trash(response),
    })
}

pub(crate) async fn run_filesystem_revisions(
    kind: CommandKind,
    config_path: &Path,
    args: FilesystemRevisionsArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let allow_root = false;
    let spec = namespace_path(&context.namespace, &args.path, allow_root)
        .map_err(|error| context.fail(kind, error))?;
    let response = context
        .target
        .list_file_revisions_page(&spec, args.limit, args.cursor.as_deref())
        .await
        .map_err(|error| context.fail(kind, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::FileRevisions {
            target: render_target(&context.namespace, spec.absolute_path()),
            revisions: response.revisions,
            next_cursor: response.next_cursor,
        },
    })
}

pub(crate) async fn run_filesystem_put(
    kind: CommandKind,
    config_path: &Path,
    args: FilesystemPutArgs,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let local_path = PathBuf::from(&args.local_path);
    if local_path == Path::new(STDIN_PATH) {
        return run_filesystem_put_stdin(kind, args, context).await;
    }

    let metadata = fs::metadata(&local_path)
        .map_err(|error| context.fail(kind, CliError::io_for_path(&local_path, error)))?;
    if args.recursive {
        if !metadata.is_dir() {
            return Err(context.fail(
                kind,
                CliError::invalid_input(format!(
                    "`{}` is not a directory; drop -r to upload one file",
                    local_path.display()
                )),
            ));
        }
        if args.commit_id.is_some() {
            return Err(context.fail(
                kind,
                CliError::invalid_input(
                    "--commit-id names one commit; a recursive upload makes one commit per file",
                ),
            ));
        }
        let remote_root = match args.remote_path {
            Some(path) => parse_user_path(&path, true),
            None => default_remote_put_path(&local_path),
        }
        .map_err(|error| context.fail(kind, error))?;
        return recursive::run_put_tree(
            kind,
            &context,
            &local_path,
            remote_root.as_str(),
            args.force,
            args.message.clone(),
            runtime,
        )
        .await;
    }
    if metadata.is_dir() {
        return Err(context.fail(
            kind,
            CliError::invalid_input(format!(
                "`{}` is a directory; use `loonfs put -r` to upload the tree",
                local_path.display()
            )),
        ));
    }

    let local_leaf = local_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .ok_or_else(|| {
            context.fail(
                kind,
                CliError::invalid_input(format!(
                    "unable to derive remote target from `{}`",
                    local_path.display()
                )),
            )
        })?;
    let remote_path = match args.remote_path.as_deref() {
        // A trailing slash names the directory the file lands in — the
        // cp/rsync habit — while a plain path is the full destination.
        Some(path) => destination_user_path(path, &local_leaf, true),
        None => default_remote_put_path(&local_path),
    }
    .map_err(|error| context.fail(kind, error))?;
    let spec = NamespacePath::new(context.namespace.clone(), remote_path);
    let payload = LocalPayload::file(&local_path, metadata.len());
    let options = put_file_options(&args).map_err(|error| context.fail(kind, error))?;
    commit_put(kind, &context, &spec, &payload, &options).await
}

/// `loonfs put - <remote>`: standard input, whose length is not knowable, so
/// it is always read once and never held.
///
/// The remote path has to be spelled out. Every other `put` derives a
/// default from the local file's name, and a pipe has none to derive from.
async fn run_filesystem_put_stdin(
    kind: CommandKind,
    args: FilesystemPutArgs,
    context: CommandContext,
) -> Result<CommandOutput, CommandFailure> {
    if args.recursive {
        return Err(context.fail(
            kind,
            CliError::invalid_input("`-` streams one file; a tree needs a directory"),
        ));
    }
    let Some(remote_path) = args.remote_path.as_deref() else {
        return Err(context.fail(
            kind,
            CliError::invalid_input(
                "reading from `-` needs an explicit remote path; there is no local name to \
                 derive one from",
            ),
        ));
    };
    let remote_path =
        parse_user_path(remote_path, false).map_err(|error| context.fail(kind, error))?;
    let spec = NamespacePath::new(context.namespace.clone(), remote_path);
    let options = put_file_options(&args).map_err(|error| context.fail(kind, error))?;
    commit_put(kind, &context, &spec, &LocalPayload::Stdin, &options).await
}

/// Writes one payload and renders what the commit did.
///
/// The payload decides how it travels: one that is small enough to hold
/// goes as bytes, and one that is large — or that cannot say how large it
/// is — is read once, in pieces, whichever transport the profile selects.
async fn commit_put(
    kind: CommandKind,
    context: &CommandContext,
    spec: &NamespacePath,
    payload: &LocalPayload,
    options: &PutFileOptions,
) -> Result<CommandOutput, CommandFailure> {
    let result = match payload.holdable_file() {
        Some(path) => {
            let bytes = read_whole_file(path)
                .await
                .map_err(|error| context.fail(kind, error))?;
            context.target.put_file_bytes(spec, &bytes, options).await
        }
        None => context.target.put_file_stream(spec, payload, options).await,
    }
    .map_err(|error| context.fail(kind, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name.clone()),
        mode: Some(context.mode.clone()),
        data: CommandData::FileMutation {
            target: render_target(&context.namespace, spec.absolute_path()),
            committed_seq: result.committed_seq,
            commit_id: result.commit_id,
            inode_id: None,
        },
    })
}

fn put_file_options(args: &FilesystemPutArgs) -> Result<PutFileOptions, CliError> {
    let commit_id = parse_commit_id_arg(args.commit_id.as_deref())?;
    let expected_revision_no = args.expected_revision.map(RevisionNo);
    // The revision guard is a stronger replace statement, so it implies
    // --force rather than demanding both flags.
    let behavior = if args.force || expected_revision_no.is_some() {
        DestinationBehavior::Replace
    } else {
        DestinationBehavior::NoReplace
    };
    Ok(PutFileOptions {
        behavior,
        commit_id,
        message: args.message.clone(),
        expected_revision_no,
    })
}

pub(crate) async fn run_filesystem_rm(
    kind: CommandKind,
    config_path: &Path,
    args: FilesystemRmArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let allow_root = false;
    let spec = namespace_path(&context.namespace, &args.path, allow_root)
        .map_err(|error| context.fail(kind, error))?;
    let commit_id = parse_commit_id_arg(args.commit_id.as_deref())
        .map_err(|error| context.fail(kind, error))?;
    // Resolve the inode before deleting: the id is half of the recovery
    // handle `loonfs undelete` needs. The delete then carries it as an
    // expectation, so a rebinding racing this command fails the delete
    // instead of removing (and mis-reporting) a different inode.
    let deleted_inode = context
        .target
        .stat_path(&spec)
        .await
        .map_err(|error| context.fail(kind, error))?
        .inode_id;
    let behavior = if args.recursive {
        DeleteDirectoryBehavior::Recursive
    } else {
        DeleteDirectoryBehavior::NonRecursive
    };
    let options = DeleteOptions {
        behavior,
        expected_inode_id: Some(deleted_inode),
        commit_id,
        message: args.message.clone(),
    };
    let result = context
        .target
        .delete_path(&spec, &options)
        .await
        .map_err(|error| context.fail(kind, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::FileMutation {
            target: render_target(&context.namespace, spec.absolute_path()),
            committed_seq: result.committed_seq,
            commit_id: result.commit_id,
            inode_id: Some(deleted_inode),
        },
    })
}

pub(crate) async fn run_filesystem_restore(
    kind: CommandKind,
    config_path: &Path,
    args: FilesystemRestoreArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let allow_root = false;
    let spec = namespace_path(&context.namespace, &args.path, allow_root)
        .map_err(|error| context.fail(kind, error))?;
    let commit_id = parse_commit_id_arg(args.commit_id.as_deref())
        .map_err(|error| context.fail(kind, error))?;
    let result = context
        .target
        .restore_file_revision(
            &spec,
            RevisionNo(args.revision),
            &loonfs_client::RestoreRevisionOptions {
                commit_id,
                message: args.message.clone(),
            },
        )
        .await
        .map_err(|error| context.fail(kind, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::FileMutation {
            target: render_target(&context.namespace, spec.absolute_path()),
            committed_seq: result.committed_seq,
            commit_id: result.commit_id,
            inode_id: None,
        },
    })
}

pub(crate) async fn run_filesystem_undelete(
    kind: CommandKind,
    config_path: &Path,
    args: FilesystemUndeleteArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let allow_root = false;
    let spec = namespace_path(&context.namespace, &args.path, allow_root)
        .map_err(|error| context.fail(kind, error))?;
    let commit_id = parse_commit_id_arg(args.commit_id.as_deref())
        .map_err(|error| context.fail(kind, error))?;
    let result = context
        .target
        .undelete(
            &spec,
            loonfs_api::InodeId(args.inode),
            loonfs_api::ChangeSeq(args.deleted_at),
            &loonfs_client::UndeleteOptions {
                commit_id,
                message: args.message.clone(),
            },
        )
        .await
        .map_err(|error| context.fail(kind, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::FileMutation {
            target: render_target(&context.namespace, spec.absolute_path()),
            committed_seq: result.committed_seq,
            commit_id: result.commit_id,
            inode_id: Some(loonfs_api::InodeId(args.inode)),
        },
    })
}

pub(crate) async fn run_filesystem_mkdir(
    kind: CommandKind,
    config_path: &Path,
    args: FilesystemMkdirArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let allow_root = false;
    let spec = namespace_path(&context.namespace, &args.path, allow_root)
        .map_err(|error| context.fail(kind, error))?;
    let commit_id = parse_commit_id_arg(args.commit_id.as_deref())
        .map_err(|error| context.fail(kind, error))?;
    let options = CreateDirectoryOptions {
        parents: args.parents,
        commit_id,
        message: args.message.clone(),
    };
    let result = match context.target.create_directory(&spec, &options).await {
        Ok(result) => result,
        // Unix `mkdir -p` treats a directory that is already there as
        // success. The conflict is what says the path is occupied, and a
        // stat then says by what: a directory is the state `-p` asked for,
        // anything else is the conflict the caller has to hear about.
        // Reading the conflict rather than pre-checking keeps the ordinary
        // path one round trip, and a pre-check would race just the same.
        Err(error) if args.parents && error.code == ErrorCode::PathConflict.as_str() => {
            let existing = context
                .target
                .stat_path(&spec)
                .await
                .map_err(|_| context.fail(kind, error.clone()))?;
            if existing.inode_kind != InodeKind::Directory {
                return Err(context.fail(kind, error));
            }
            return Ok(CommandOutput {
                kind,
                profile: Some(context.profile_name),
                mode: Some(context.mode),
                data: CommandData::DirectoryAlreadyExists {
                    target: render_target(&context.namespace, spec.absolute_path()),
                    inode_id: existing.inode_id,
                    head_seq: existing.head_seq,
                },
            });
        }
        Err(error) => return Err(context.fail(kind, error)),
    };

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::FileMutation {
            target: render_target(&context.namespace, spec.absolute_path()),
            committed_seq: result.committed_seq,
            commit_id: result.commit_id,
            inode_id: None,
        },
    })
}

pub(crate) async fn run_filesystem_mv(
    kind: CommandKind,
    config_path: &Path,
    args: FilesystemTransferArgs,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    run_filesystem_transfer(kind, config_path, args, TransferKind::Move, runtime).await
}

pub(crate) async fn run_filesystem_cp(
    kind: CommandKind,
    config_path: &Path,
    args: FilesystemTransferArgs,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    run_filesystem_transfer(kind, config_path, args, TransferKind::Copy, runtime).await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransferKind {
    Move,
    Copy,
}

/// Applies the `cp`/`mv` habit for a destination that is an existing
/// directory: the item lands inside it under its own name.
///
/// A trailing slash already said "into this directory" before the command
/// ran, and this covers the spelling without one, which Unix reads the same
/// way. Only an existing directory redirects: a destination that does not
/// exist is the full path the caller typed, and a destination that is a file
/// stays the overwrite question `--force` answers.
///
/// The stat can go stale between here and the commit. That is the same
/// window `cp` has on any filesystem, and losing it fails the transfer
/// rather than writing somewhere unexpected — the commit still names the
/// exact path resolved here.
async fn resolve_transfer_destination(
    context: &CommandContext,
    named: NamespacePath,
    source_leaf: &str,
) -> Result<NamespacePath, CliError> {
    let Ok(existing) = context.target.stat_path(&named).await else {
        // Absent, or unreadable for a reason the transfer itself will
        // report: either way this is not a directory to land inside.
        return Ok(named);
    };
    if existing.inode_kind != InodeKind::Directory {
        return Ok(named);
    }
    let leaf = loonfs_api::DisplayName::parse(source_leaf)
        .map_err(|error| CliError::invalid_input(error.to_string()))?;
    Ok(NamespacePath::new(
        context.namespace.clone(),
        named.absolute_path().join(&leaf),
    ))
}

async fn run_filesystem_transfer(
    kind: CommandKind,
    config_path: &Path,
    args: FilesystemTransferArgs,
    transfer_kind: TransferKind,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    if args.recursive && transfer_kind == TransferKind::Move {
        return Err(context.fail(
            kind,
            CliError::invalid_input("mv moves a directory in one commit; -r is not needed"),
        ));
    }
    let allow_root = false;
    let from = namespace_path(&context.namespace, &args.source_path, allow_root)
        .map_err(|error| context.fail(kind, error))?;
    let source_leaf = from
        .absolute_path()
        .final_component()
        .map(|component| component.as_str().to_owned())
        .ok_or_else(|| {
            context.fail(
                kind,
                CliError::invalid_input("root path is not allowed for this command"),
            )
        })?;
    let named_destination = destination_user_path(&args.destination_path, &source_leaf, true)
        .map(|path| NamespacePath::new(context.namespace.clone(), path))
        .map_err(|error| context.fail(kind, error))?;
    // A destination spelled with a trailing slash already named the
    // directory to land in, and the leaf is already appended; looking again
    // would append it twice.
    let to = if directory_intent(&args.destination_path) || args.destination_path == "/" {
        named_destination
    } else {
        resolve_transfer_destination(&context, named_destination, &source_leaf)
            .await
            .map_err(|error| context.fail(kind, error))?
    };

    let commit_id = parse_commit_id_arg(args.commit_id.as_deref())
        .map_err(|error| context.fail(kind, error))?;
    let result = if transfer_kind == TransferKind::Copy {
        let entry = context
            .target
            .stat_path(&from)
            .await
            .map_err(|error| context.fail(kind, error))?;
        if args.recursive {
            if entry.inode_kind != InodeKind::Directory {
                return Err(context.fail(
                    kind,
                    CliError::invalid_input(format!(
                        "`{}` is not a directory; drop -r to copy one file",
                        from.absolute_path()
                    )),
                ));
            }
            if args.commit_id.is_some() {
                return Err(context.fail(
                    kind,
                    CliError::invalid_input(
                        "--commit-id names one commit; a recursive copy makes one commit per item",
                    ),
                ));
            }
            return recursive::run_copy_tree(
                kind,
                &context,
                from.absolute_path().as_str(),
                to.absolute_path().as_str(),
                args.force,
                args.message.clone(),
                runtime,
            )
            .await;
        }
        if entry.inode_kind == InodeKind::Directory {
            return Err(context.fail(
                kind,
                CliError::invalid_input(format!(
                    "`{}` is a directory; use `loonfs cp -r` to copy the tree",
                    from.absolute_path()
                )),
            ));
        }
        let behavior = if args.force {
            DestinationBehavior::Replace
        } else {
            DestinationBehavior::NoReplace
        };
        context
            .target
            .copy_path(
                &from,
                &to,
                &loonfs_client::CopyOptions {
                    behavior,
                    commit_id,
                    message: args.message.clone(),
                },
            )
            .await
    } else {
        let behavior = if args.force {
            DestinationBehavior::Replace
        } else {
            DestinationBehavior::NoReplace
        };
        context
            .target
            .move_path(
                &from,
                &to,
                &loonfs_client::MoveOptions {
                    behavior,
                    commit_id,
                    message: args.message.clone(),
                },
            )
            .await
    }
    .map_err(|error| context.fail(kind, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::PathMove {
            from: render_target(&context.namespace, from.absolute_path()),
            to: render_target(&context.namespace, to.absolute_path()),
            committed_seq: result.committed_seq,
            commit_id: result.commit_id,
        },
    })
}
