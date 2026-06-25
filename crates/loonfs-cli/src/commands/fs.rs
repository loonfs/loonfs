use super::context::{
    default_remote_put_path, destination_path_for_get, fail, namespace_path,
    normalize_absolute_path, render_target, resolve_command_context,
};
use super::output::{CommandData, CommandFailure, CommandOutput};
use crate::args::{
    CommandKind, FilesystemCatArgs, FilesystemGetArgs, FilesystemLsArgs, FilesystemMoveArgs,
    FilesystemPathArgs, FilesystemPutArgs, FilesystemRestoreArgs, FilesystemRevisionsArgs,
    RuntimeBehavior,
};
use crate::error::CliError;
use loonfs_api::{InodeKind, RevisionNo};
use loonfs_client::NamespacePath;
use std::fs;
use std::path::{Path, PathBuf};

// --- filesystem ---

pub(crate) fn run_filesystem_ls(
    kind: CommandKind,
    args: FilesystemLsArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target)?;
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
    let entries = context.target.backend().list_path(&spec).map_err(|error| {
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

pub(crate) fn run_filesystem_stat(
    kind: CommandKind,
    args: FilesystemPathArgs,
) -> Result<CommandOutput, CommandFailure> {
    run_filesystem_path_lookup(kind, args, |backend, spec| {
        backend.stat_path(spec).map(CommandData::PathEntry)
    })
}

pub(crate) fn run_filesystem_cat(
    kind: CommandKind,
    args: FilesystemCatArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target)?;
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
        Some(revision_no) => context
            .target
            .backend()
            .read_file_revision_bytes(&spec, revision_no),
        None => context.target.backend().read_file_bytes(&spec),
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

pub(crate) fn run_filesystem_get(
    kind: CommandKind,
    args: FilesystemGetArgs,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target)?;
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
    let entry = context.target.backend().stat_path(&spec).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;
    if entry.inode_kind == InodeKind::Dir {
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
        Some(revision_no) => context
            .target
            .backend()
            .read_file_revision_bytes(&spec, revision_no),
        None => context.target.backend().read_file_bytes(&spec),
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
            let destination =
                destination_path_for_get(&spec.absolute_path, other).map_err(|error| {
                    fail(
                        kind,
                        Some(context.profile_name.clone()),
                        Some(context.mode.clone()),
                        error,
                    )
                })?;
            fs::write(&destination, &bytes).map_err(|error| {
                fail(
                    kind,
                    Some(context.profile_name.clone()),
                    Some(context.mode.clone()),
                    CliError::io(error),
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

pub(crate) fn run_filesystem_revisions(
    kind: CommandKind,
    args: FilesystemRevisionsArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target)?;
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

pub(crate) fn run_filesystem_put(
    kind: CommandKind,
    args: FilesystemPutArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target)?;
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
    let result = context
        .target
        .backend()
        .put_file_bytes(&spec, &bytes, args.force)
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
        },
    })
}

pub(crate) fn run_filesystem_rm(
    kind: CommandKind,
    args: FilesystemPathArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target)?;
    let spec = namespace_path(&context.namespace, &args.path, false).map_err(|error| {
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
        .delete_path(&spec)
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
        },
    })
}

pub(crate) fn run_filesystem_restore(
    kind: CommandKind,
    args: FilesystemRestoreArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target)?;
    let spec = namespace_path(&context.namespace, &args.path, false).map_err(|error| {
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
        .restore_file_revision(&spec, RevisionNo(args.revision))
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
        },
    })
}

pub(crate) fn run_filesystem_mkdir(
    kind: CommandKind,
    args: FilesystemPathArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target)?;
    let spec = namespace_path(&context.namespace, &args.path, false).map_err(|error| {
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
        .create_dir(&spec)
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
        },
    })
}

pub(crate) fn run_filesystem_mv(
    kind: CommandKind,
    args: FilesystemMoveArgs,
) -> Result<CommandOutput, CommandFailure> {
    run_filesystem_move(kind, args, false)
}

pub(crate) fn run_filesystem_cp(
    kind: CommandKind,
    args: FilesystemMoveArgs,
) -> Result<CommandOutput, CommandFailure> {
    run_filesystem_move(kind, args, true)
}

fn run_filesystem_path_lookup<F>(
    kind: CommandKind,
    args: FilesystemPathArgs,
    op: F,
) -> Result<CommandOutput, CommandFailure>
where
    F: FnOnce(&dyn crate::backend::Backend, &NamespacePath) -> Result<CommandData, CliError>,
{
    let context = resolve_command_context(kind, &args.target)?;
    let spec = namespace_path(&context.namespace, &args.path, false).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;
    let data = op(context.target.backend(), &spec).map_err(|error| {
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
        data,
    })
}

fn run_filesystem_move(
    kind: CommandKind,
    args: FilesystemMoveArgs,
    copy: bool,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target)?;
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

    let result = if copy {
        let entry = context.target.backend().stat_path(&from).map_err(|error| {
            fail(
                kind,
                Some(context.profile_name.clone()),
                Some(context.mode.clone()),
                error,
            )
        })?;
        if entry.inode_kind == InodeKind::Dir {
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
        context.target.backend().copy_path(&from, &to)
    } else {
        context.target.backend().move_path(&from, &to)
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
        },
    })
}
