//! Filesystem commands: ls, stat, get, put, mkdir, rm, mv, cp, revisions,
//! and grep.

use super::context::{
    default_remote_put_path, destination_path_for_get, destination_user_path, fail, namespace_path,
    parse_user_path, render_target, resolve_command_context,
};
use super::output::{CommandData, CommandFailure, CommandOutput};
use super::recursive;
use crate::args::{
    CommandKind, FilesystemCatArgs, FilesystemGetArgs, FilesystemGrepArgs, FilesystemLsArgs,
    FilesystemMkdirArgs, FilesystemPathArgs, FilesystemPutArgs, FilesystemRestoreArgs,
    FilesystemRevisionsArgs, FilesystemRmArgs, FilesystemTransferArgs, FilesystemUndeleteArgs,
    RuntimeBehavior, TrashArgs,
};
use crate::error::CliError;
use loonfs_api::{CommitId, DeleteDirectoryBehavior, DestinationBehavior, InodeKind, RevisionNo};
use loonfs_client::{CreateDirectoryOptions, DeleteOptions, NamespacePath, PutFileOptions};
use std::fs;
use std::io::Write;
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

/// Writes via a same-directory temp file and an atomic rename, so a failed
/// or interrupted download never leaves a truncated file at the target.
pub(super) fn write_local_file_atomically(
    destination: &Path,
    bytes: &[u8],
    force: bool,
) -> std::io::Result<()> {
    let file_name = destination
        .file_name()
        .ok_or_else(|| std::io::Error::other("destination has no file name"))?;
    let mut prefix = std::ffi::OsString::from(".");
    prefix.push(file_name);
    prefix.push(".loon-partial-");
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut temp = tempfile::Builder::new()
        .prefix(&prefix)
        .tempfile_in(parent)?;
    temp.write_all(bytes)?;
    temp.flush()?;
    let persisted = if force {
        temp.persist(destination)
    } else {
        temp.persist_noclobber(destination)
    };
    persisted.map(|_| ()).map_err(|error| error.error)
}

pub(crate) async fn run_filesystem_ls(
    kind: CommandKind,
    args: FilesystemLsArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
    let allow_root = true;
    let spec = namespace_path(
        &context.namespace,
        args.path.as_deref().unwrap_or("/"),
        allow_root,
    )
    .map_err(|error| context.fail(kind, error))?;
    let entries = context
        .target
        .list_path_entries_all(&spec)
        .await
        .map_err(|error| context.fail(kind, error))?;
    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::PathEntries { entries },
    })
}

pub(crate) async fn run_filesystem_stat(
    kind: CommandKind,
    args: FilesystemPathArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
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
    args: FilesystemGrepArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
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
    let (namespace_id, head_seq, built_through_seq) = loop {
        let response = context
            .target
            .grep(&context.namespace, &request)
            .await
            .map_err(|error| context.fail(kind, error))?;
        matches.extend(response.matches);
        tail_scanned &= response.tail_scanned;
        match response.next_cursor {
            Some(cursor) => request.cursor = Some(cursor),
            None => {
                // The final page's snapshot describes the completed query.
                break (
                    response.namespace_id,
                    response.head_seq,
                    response.built_through_seq,
                );
            }
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
        },
    })
}

pub(crate) async fn run_filesystem_cat(
    kind: CommandKind,
    args: FilesystemCatArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
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
    args: FilesystemGetArgs,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
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
                "`{}` is a directory; use `loon get -r` to download the tree",
                spec.absolute_path()
            )),
        ));
    }

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
    let data = match args.local_destination.as_deref() {
        Some("-") => CommandData::StreamBytes(bytes),
        other => {
            let derived_name = other.is_none();
            let destination = destination_path_for_get(spec.absolute_path().as_str(), other)
                .map_err(|error| context.fail(kind, error))?;
            // The local working copy is the one thing this CLI touches
            // that has no revision history behind it, so clobbering it is
            // opt-in. `persist_noclobber` closes the race between checking
            // and installing the completed temporary file.
            write_local_file_atomically(&destination, &bytes, args.force).map_err(|error| {
                if !args.force && error.kind() == std::io::ErrorKind::AlreadyExists {
                    return context.fail(kind, CliError::destination_exists(&destination));
                }
                let mut error = CliError::io_for_path(&destination, error);
                if derived_name {
                    error.message.push_str(
                        "; if the remote name exceeds local filesystem limits, pass an \
                         explicit destination or `-` for stdout",
                    );
                }
                context.fail(kind, error)
            })?;
            CommandData::FileTransfer {
                target: render_target(&context.namespace, spec.absolute_path()),
                destination: destination.display().to_string(),
                bytes_written: bytes.len() as u64,
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

pub(crate) async fn run_filesystem_trash(
    kind: CommandKind,
    args: TrashArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
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
    args: FilesystemRevisionsArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
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
    args: FilesystemPutArgs,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
    let local_path = PathBuf::from(&args.local_path);
    if local_path == Path::new("-") {
        return Err(fail(
            kind,
            Some(context.profile_name),
            Some(context.mode),
            CliError::invalid_input("`-` is not supported for `put`"),
        ));
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
                "`{}` is a directory; use `loon put -r` to upload the tree",
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
    let remote_path = match args.remote_path {
        // A trailing slash names the directory the file lands in — the
        // cp/rsync habit — while a plain path is the full destination.
        Some(path) => destination_user_path(&path, &local_leaf, true),
        None => default_remote_put_path(&local_path),
    }
    .map_err(|error| context.fail(kind, error))?;
    let spec = NamespacePath::new(context.namespace.clone(), remote_path);
    let bytes = fs::read(&local_path)
        .map_err(|error| context.fail(kind, CliError::io_for_path(&local_path, error)))?;
    let commit_id = parse_commit_id_arg(args.commit_id.as_deref())
        .map_err(|error| context.fail(kind, error))?;
    let expected_revision_no = args.expected_revision.map(RevisionNo);
    // The revision guard is a stronger replace statement, so it implies
    // --force rather than demanding both flags.
    let behavior = if args.force || expected_revision_no.is_some() {
        DestinationBehavior::Replace
    } else {
        DestinationBehavior::NoReplace
    };
    let options = PutFileOptions {
        behavior,
        commit_id,
        message: args.message.clone(),
        expected_revision_no,
    };
    let result = context
        .target
        .put_file_bytes(&spec, &bytes, &options)
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

pub(crate) async fn run_filesystem_rm(
    kind: CommandKind,
    args: FilesystemRmArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
    let allow_root = false;
    let spec = namespace_path(&context.namespace, &args.path, allow_root)
        .map_err(|error| context.fail(kind, error))?;
    let commit_id = parse_commit_id_arg(args.commit_id.as_deref())
        .map_err(|error| context.fail(kind, error))?;
    // Resolve the inode before deleting: the id is half of the recovery
    // handle `loon undelete` needs. The delete then carries it as an
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
    args: FilesystemRestoreArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
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
            &loonfs_client::CommitOptions {
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
    args: FilesystemUndeleteArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
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
            &loonfs_client::CommitOptions {
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
    args: FilesystemMkdirArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
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
    let result = context
        .target
        .create_directory(&spec, &options)
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

pub(crate) async fn run_filesystem_mv(
    kind: CommandKind,
    args: FilesystemTransferArgs,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    run_filesystem_transfer(kind, args, TransferKind::Move, runtime).await
}

pub(crate) async fn run_filesystem_cp(
    kind: CommandKind,
    args: FilesystemTransferArgs,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    run_filesystem_transfer(kind, args, TransferKind::Copy, runtime).await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransferKind {
    Move,
    Copy,
}

async fn run_filesystem_transfer(
    kind: CommandKind,
    args: FilesystemTransferArgs,
    transfer_kind: TransferKind,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
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
    let to = destination_user_path(&args.destination_path, &source_leaf, true)
        .map(|path| NamespacePath::new(context.namespace.clone(), path))
        .map_err(|error| context.fail(kind, error))?;

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
                    "`{}` is a directory; use `loon cp -r` to copy the tree",
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
                behavior,
                &loonfs_client::CommitOptions {
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
                behavior,
                &loonfs_client::CommitOptions {
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
