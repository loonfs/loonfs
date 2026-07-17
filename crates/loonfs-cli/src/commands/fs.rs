//! Filesystem commands: ls, stat, get, put, mkdir, rm, mv, cp, revisions,
//! and grep.

use super::context::{
    default_remote_put_path, destination_path_for_get, fail, namespace_path,
    normalize_absolute_path, render_target, resolve_command_context,
};
use super::output::{CommandData, CommandFailure, CommandOutput};
use crate::args::{
    CommandKind, FilesystemCatArgs, FilesystemGetArgs, FilesystemGrepArgs, FilesystemLsArgs,
    FilesystemMkdirArgs, FilesystemPathArgs, FilesystemPathMutationArgs, FilesystemPutArgs,
    FilesystemRestoreArgs, FilesystemRevisionsArgs, FilesystemTransferArgs, FilesystemUndeleteArgs,
    RuntimeBehavior,
};
use crate::error::CliError;
use loonfs_api::{CommitId, CopyBehavior, InodeKind, MoveBehavior, PutBehavior, RevisionNo};
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
fn write_local_file_atomically(
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
    let spec = namespace_path(
        &context.namespace,
        args.path.as_deref().unwrap_or("/"),
        true,
    )
    .map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;
    let entries = context
        .target
        .backend()
        .list_path(&spec)
        .await
        .map_err(|error| {
            fail(
                kind,
                Some(context.profile_name.clone()),
                Some(context.mode.clone()),
                error,
            )
        })?;
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
    let spec = namespace_path(&context.namespace, &args.path, false).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;
    let entry = context
        .target
        .backend()
        .stat_path(&spec)
        .await
        .map_err(|error| {
            fail(
                kind,
                Some(context.profile_name.clone()),
                Some(context.mode.clone()),
                error,
            )
        })?;

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
    let mut request = loonfs_api::GrepRequest {
        pattern: args.pattern.clone(),
        case_insensitive: args.ignore_case,
        path_prefix: args.path_prefix.clone(),
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
            .backend()
            .grep(&context.namespace, &request)
            .await
            .map_err(|error| {
                fail(
                    kind,
                    Some(context.profile_name.clone()),
                    Some(context.mode.clone()),
                    error,
                )
            })?;
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
            namespace_id: namespace_id.to_string(),
            head_seq: head_seq.0,
            built_through_seq: built_through_seq.0,
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
    let spec = namespace_path(&context.namespace, &args.path, false).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;
    let revision_no = args.revision.map(RevisionNo);
    let bytes = match revision_no {
        Some(revision_no) => {
            context
                .target
                .backend()
                .read_file_revision_bytes(&spec, revision_no)
                .await
        }
        None => context.target.backend().read_file_bytes(&spec).await,
    }
    .map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;

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

    let spec = namespace_path(&context.namespace, &args.remote_path, false).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;
    let entry = context
        .target
        .backend()
        .stat_path(&spec)
        .await
        .map_err(|error| {
            fail(
                kind,
                Some(context.profile_name.clone()),
                Some(context.mode.clone()),
                error,
            )
        })?;
    if entry.inode_kind == InodeKind::Directory {
        return Err(fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            CliError::invalid_input(format!(
                "directory operations are not available for `{}`",
                spec.absolute_path
            )),
        ));
    }

    let revision_no = args.revision.map(RevisionNo);
    let bytes = match revision_no {
        Some(revision_no) => {
            context
                .target
                .backend()
                .read_file_revision_bytes(&spec, revision_no)
                .await
        }
        None => context.target.backend().read_file_bytes(&spec).await,
    }
    .map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;
    let data = match args.local_destination.as_deref() {
        Some("-") => CommandData::StreamBytes(bytes),
        other => {
            let derived_name = other.is_none();
            let destination =
                destination_path_for_get(&spec.absolute_path, other).map_err(|error| {
                    fail(
                        kind,
                        Some(context.profile_name.clone()),
                        Some(context.mode.clone()),
                        error,
                    )
                })?;
            // The local working copy is the one thing this CLI touches
            // that has no revision history behind it, so clobbering it is
            // opt-in. `persist_noclobber` closes the race between checking
            // and installing the completed temporary file.
            write_local_file_atomically(&destination, &bytes, args.force).map_err(|error| {
                if !args.force && error.kind() == std::io::ErrorKind::AlreadyExists {
                    return fail(
                        kind,
                        Some(context.profile_name.clone()),
                        Some(context.mode.clone()),
                        CliError::new(
                            "destination_exists",
                            format!(
                                "local file `{}` already exists; pass --force to overwrite",
                                destination.display()
                            ),
                        ),
                    );
                }
                let mut error = CliError::io(error);
                if derived_name {
                    error.message.push_str(
                        "; if the remote name exceeds local filesystem limits, pass an \
                         explicit destination or `-` for stdout",
                    );
                }
                fail(
                    kind,
                    Some(context.profile_name.clone()),
                    Some(context.mode.clone()),
                    error,
                )
            })?;
            CommandData::FileTransfer {
                target: render_target(&context.namespace, &spec.absolute_path),
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

pub(crate) async fn run_filesystem_revisions(
    kind: CommandKind,
    args: FilesystemRevisionsArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
    let spec = namespace_path(&context.namespace, &args.path, false).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;
    let response = context
        .target
        .backend()
        .list_file_revisions(&spec, args.limit, args.cursor.as_deref())
        .await
        .map_err(|error| {
            fail(
                kind,
                Some(context.profile_name.clone()),
                Some(context.mode.clone()),
                error,
            )
        })?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::FileRevisions {
            target: render_target(&context.namespace, &spec.absolute_path),
            revisions: response.revisions,
            next_cursor: response.next_cursor,
        },
    })
}

pub(crate) async fn run_filesystem_put(
    kind: CommandKind,
    args: FilesystemPutArgs,
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

    let metadata = fs::metadata(&local_path).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            CliError::io(error),
        )
    })?;
    if metadata.is_dir() {
        return Err(fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            CliError::invalid_input(format!(
                "directory operations are not available for `{}`",
                local_path.display()
            )),
        ));
    }

    let remote_path = match args.remote_path {
        Some(path) => normalize_absolute_path(&path, false),
        None => default_remote_put_path(&local_path),
    }
    .map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;
    let spec = namespace_path(&context.namespace, &remote_path, false).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;
    let bytes = fs::read(&local_path).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            CliError::io(error),
        )
    })?;
    let commit_id = parse_commit_id_arg(args.commit_id.as_deref()).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;
    let behavior = if args.force {
        PutBehavior::Replace
    } else {
        PutBehavior::NoReplace
    };
    let result = context
        .target
        .backend()
        .put_file_bytes(&spec, &bytes, behavior, commit_id)
        .await
        .map_err(|error| {
            fail(
                kind,
                Some(context.profile_name.clone()),
                Some(context.mode.clone()),
                error,
            )
        })?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::FileMutation {
            target: render_target(&context.namespace, &spec.absolute_path),
            committed_seq: result.committed_seq.0,
            commit_id: result.commit_id.to_string(),
            inode_id: None,
        },
    })
}

pub(crate) async fn run_filesystem_rm(
    kind: CommandKind,
    args: FilesystemPathMutationArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
    let spec = namespace_path(&context.namespace, &args.path, false).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;
    let commit_id = parse_commit_id_arg(args.commit_id.as_deref()).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;
    // Resolve the inode before deleting: the delete unbinds the path, and
    // the id is what `loon undelete` needs to recover it.
    let deleted_inode = context
        .target
        .backend()
        .stat_path(&spec)
        .await
        .ok()
        .map(|entry| entry.inode_id.0);
    let result = context
        .target
        .backend()
        .delete_path(&spec, commit_id)
        .await
        .map_err(|error| {
            fail(
                kind,
                Some(context.profile_name.clone()),
                Some(context.mode.clone()),
                error,
            )
        })?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::FileMutation {
            target: render_target(&context.namespace, &spec.absolute_path),
            committed_seq: result.committed_seq.0,
            commit_id: result.commit_id.to_string(),
            inode_id: deleted_inode,
        },
    })
}

pub(crate) async fn run_filesystem_restore(
    kind: CommandKind,
    args: FilesystemRestoreArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
    let spec = namespace_path(&context.namespace, &args.path, false).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;
    let commit_id = parse_commit_id_arg(args.commit_id.as_deref()).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;
    let result = context
        .target
        .backend()
        .restore_file_revision(&spec, RevisionNo(args.revision), commit_id)
        .await
        .map_err(|error| {
            fail(
                kind,
                Some(context.profile_name.clone()),
                Some(context.mode.clone()),
                error,
            )
        })?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::FileMutation {
            target: render_target(&context.namespace, &spec.absolute_path),
            committed_seq: result.committed_seq.0,
            commit_id: result.commit_id.to_string(),
            inode_id: None,
        },
    })
}

pub(crate) async fn run_filesystem_undelete(
    kind: CommandKind,
    args: FilesystemUndeleteArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
    let spec = namespace_path(&context.namespace, &args.path, false).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;
    let commit_id = parse_commit_id_arg(args.commit_id.as_deref()).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;
    let result = context
        .target
        .backend()
        .undelete(
            &spec,
            loonfs_api::InodeId(args.inode),
            loonfs_api::ChangeSeq(args.deleted_at),
            commit_id,
        )
        .await
        .map_err(|error| {
            fail(
                kind,
                Some(context.profile_name.clone()),
                Some(context.mode.clone()),
                error,
            )
        })?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::FileMutation {
            target: render_target(&context.namespace, &spec.absolute_path),
            committed_seq: result.committed_seq.0,
            commit_id: result.commit_id.to_string(),
            inode_id: Some(args.inode),
        },
    })
}

pub(crate) async fn run_filesystem_mkdir(
    kind: CommandKind,
    args: FilesystemMkdirArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
    let spec = namespace_path(&context.namespace, &args.path, false).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;
    let commit_id = parse_commit_id_arg(args.commit_id.as_deref()).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;
    let result = context
        .target
        .backend()
        .create_directory(&spec, args.parents, commit_id)
        .await
        .map_err(|error| {
            fail(
                kind,
                Some(context.profile_name.clone()),
                Some(context.mode.clone()),
                error,
            )
        })?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::FileMutation {
            target: render_target(&context.namespace, &spec.absolute_path),
            committed_seq: result.committed_seq.0,
            commit_id: result.commit_id.to_string(),
            inode_id: None,
        },
    })
}

pub(crate) async fn run_filesystem_mv(
    kind: CommandKind,
    args: FilesystemTransferArgs,
) -> Result<CommandOutput, CommandFailure> {
    run_filesystem_transfer(kind, args, false).await
}

pub(crate) async fn run_filesystem_cp(
    kind: CommandKind,
    args: FilesystemTransferArgs,
) -> Result<CommandOutput, CommandFailure> {
    run_filesystem_transfer(kind, args, true).await
}

async fn run_filesystem_transfer(
    kind: CommandKind,
    args: FilesystemTransferArgs,
    copy: bool,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
    let from = namespace_path(&context.namespace, &args.source_path, false).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;
    let to = namespace_path(&context.namespace, &args.dest_path, false).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;

    let commit_id = parse_commit_id_arg(args.commit_id.as_deref()).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;
    let result = if copy {
        let entry = context
            .target
            .backend()
            .stat_path(&from)
            .await
            .map_err(|error| {
                fail(
                    kind,
                    Some(context.profile_name.clone()),
                    Some(context.mode.clone()),
                    error,
                )
            })?;
        if entry.inode_kind == InodeKind::Directory {
            return Err(fail(
                kind,
                Some(context.profile_name.clone()),
                Some(context.mode.clone()),
                CliError::invalid_input(format!(
                    "directory operations are not available for `{}`",
                    from.absolute_path
                )),
            ));
        }
        let behavior = if args.force {
            CopyBehavior::Replace
        } else {
            CopyBehavior::NoReplace
        };
        context
            .target
            .backend()
            .copy_path(&from, &to, behavior, commit_id)
            .await
    } else {
        let behavior = if args.force {
            MoveBehavior::Replace
        } else {
            MoveBehavior::NoReplace
        };
        context
            .target
            .backend()
            .move_path(&from, &to, behavior, commit_id)
            .await
    }
    .map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::PathMove {
            from: render_target(&context.namespace, &from.absolute_path),
            to: render_target(&context.namespace, &to.absolute_path),
            committed_seq: result.committed_seq.0,
            commit_id: result.commit_id.to_string(),
        },
    })
}
