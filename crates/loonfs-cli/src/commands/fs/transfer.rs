//! The `mv` and `cp` commands.

use super::mutate::{commit_options, destination_behavior};
use crate::args::{FilesystemTransferArgs, RuntimeBehavior};
use crate::commands::context::{
    destination_user_path, directory_intent, namespace_path, render_target, Invocation,
    NamespaceContext, RootPath,
};
use crate::commands::output::{CommandData, CommandFailure, CommandOutput};
use crate::commands::recursive;
use crate::error::{CliError, Result};
use loonfs_client::NamespacePath;
use loonfs_types::{Commit, DestinationBehavior, EntryInodeKind};

pub(crate) async fn run_filesystem_mv(
    invocation: &Invocation,
    args: FilesystemTransferArgs,
) -> Result<CommandOutput, CommandFailure> {
    run_filesystem_transfer(invocation, args, TransferKind::Move).await
}

pub(crate) async fn run_filesystem_cp(
    invocation: &Invocation,
    args: FilesystemTransferArgs,
) -> Result<CommandOutput, CommandFailure> {
    run_filesystem_transfer(invocation, args, TransferKind::Copy).await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransferKind {
    Move,
    Copy,
}

/// Applies the `cp`/`mv` habit for a destination that is an existing
/// directory: the item is placed inside it under its own name.
///
/// A trailing slash already said "into this directory" before the command
/// ran, and this covers the spelling without one, which Unix reads the same
/// way. Only an existing directory redirects: a destination that does not
/// exist is the full path the caller typed, and a destination that is a file
/// stays the overwrite question `--force` settles.
///
/// The stat can go stale between here and the commit. That is the same
/// window `cp` has on any filesystem, and losing it fails the transfer
/// rather than writing somewhere unexpected — the commit still names the
/// exact path resolved here.
async fn resolve_transfer_destination(
    context: &NamespaceContext,
    named: NamespacePath,
    source_leaf: &str,
) -> Result<NamespacePath> {
    let Ok(existing) = context.profile.target.stat_without_attributes(&named).await else {
        // Absent, or unreadable for a reason the transfer itself will
        // report: either way this is not a directory to place the item inside.
        return Ok(named);
    };
    if existing.inode_kind() != EntryInodeKind::Directory {
        return Ok(named);
    }
    let leaf = loonfs_types::DisplayName::parse(source_leaf)
        .map_err(|error| CliError::invalid_request(error.to_string()).with_param("source_path"))?;
    Ok(NamespacePath::new(
        context.namespace().clone(),
        named.absolute_path().join(&leaf),
    ))
}

async fn run_filesystem_transfer(
    invocation: &Invocation,
    args: FilesystemTransferArgs,
    transfer_kind: TransferKind,
) -> Result<CommandOutput, CommandFailure> {
    let context = NamespaceContext::resolve(invocation, args.actor.actor_id.as_ref()).await?;
    if args.recursive && transfer_kind == TransferKind::Move {
        return Err(context.fail(
            CliError::invalid_request("mv moves a directory in one commit; -r is not needed")
                .with_param("-r"),
        ));
    }
    let root = RootPath::Refused;
    let from = namespace_path(context.namespace(), "source_path", &args.source_path, root)
        .map_err(|error| context.fail(error))?;
    let source_leaf = from
        .absolute_path()
        .final_component()
        .map(|component| component.as_str().to_owned())
        .ok_or_else(|| {
            context.fail(
                CliError::invalid_request("root path is not allowed for this command")
                    .with_param("source_path"),
            )
        })?;
    let named_destination = destination_user_path(
        "destination_path",
        "source_path",
        &args.destination_path,
        &source_leaf,
        RootPath::Allowed,
    )
    .map(|path| NamespacePath::new(context.namespace().clone(), path))
    .map_err(|error| context.fail(error))?;
    // A destination spelled with a trailing slash already named the
    // directory to place the item in, and the leaf is already appended; looking again
    // would append it twice.
    let to = if directory_intent(&args.destination_path) || args.destination_path == "/" {
        named_destination
    } else {
        resolve_transfer_destination(&context, named_destination, &source_leaf)
            .await
            .map_err(|error| context.fail(error))?
    };

    match transfer_kind {
        TransferKind::Copy => run_copy(&context, &from, &to, &args, invocation.behavior).await,
        TransferKind::Move => run_move(&context, &from, &to, &args).await,
    }
}

/// Copies one file with `cp`, or a directory tree with `cp -r`.
async fn run_copy(
    context: &NamespaceContext,
    from: &NamespacePath,
    to: &NamespacePath,
    args: &FilesystemTransferArgs,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let entry = context
        .profile
        .target
        .stat_without_attributes(from)
        .await
        .map_err(|error| context.fail(error))?;
    if args.recursive {
        if entry.inode_kind() != EntryInodeKind::Directory {
            return Err(context.fail(
                CliError::invalid_request(format!(
                    "`{}` is not a directory; drop -r to copy one file",
                    from.absolute_path()
                ))
                .with_param("-r"),
            ));
        }
        if args.commit.commit_id.is_some() {
            return Err(context.fail(
                CliError::invalid_request(
                    "`--commit-id` names one commit; a recursive copy makes one commit per item",
                )
                .with_param("--commit-id"),
            ));
        }
        return recursive::run_copy_tree(
            context,
            from.absolute_path(),
            to.absolute_path(),
            destination_behavior(args.force),
            args.commit.message.clone(),
            runtime,
        )
        .await;
    }
    if entry.inode_kind() == EntryInodeKind::Directory {
        return Err(context.fail(
            CliError::invalid_request(format!(
                "`{}` is a directory; use `loonfs cp -r` to copy the tree",
                from.absolute_path()
            ))
            .with_param("source_path"),
        ));
    }
    let commit = context
        .profile
        .target
        .client
        .copy_path_with_options(
            from,
            to,
            context.actor(),
            &loonfs_client::CopyOptions {
                behavior: transfer_behavior(args),
                commit: commit_options(&args.commit),
                expected_destination_inode_id: args.expected_destination_inode_id,
                expected_destination_revision_no: args.expected_destination_revision,
            },
        )
        .await
        .map_err(|error| context.fail(error))?;
    Ok(path_move_output(context, from, to, &commit))
}

/// Moves one file or directory with `mv`, in one commit.
async fn run_move(
    context: &NamespaceContext,
    from: &NamespacePath,
    to: &NamespacePath,
    args: &FilesystemTransferArgs,
) -> Result<CommandOutput, CommandFailure> {
    let commit = context
        .profile
        .target
        .client
        .move_path_with_options(
            from,
            to,
            context.actor(),
            &loonfs_client::MoveOptions {
                behavior: transfer_behavior(args),
                commit: commit_options(&args.commit),
                expected_destination_inode_id: args.expected_destination_inode_id,
                expected_destination_revision_no: args.expected_destination_revision,
            },
        )
        .await
        .map_err(|error| context.fail(error))?;
    Ok(path_move_output(context, from, to, &commit))
}

/// Replaces an existing destination when `--force` or a destination
/// precondition asks for one.
fn transfer_behavior(args: &FilesystemTransferArgs) -> DestinationBehavior {
    if args.force
        || args.expected_destination_inode_id.is_some()
        || args.expected_destination_revision.is_some()
    {
        DestinationBehavior::Replace
    } else {
        DestinationBehavior::NoReplace
    }
}

fn path_move_output(
    context: &NamespaceContext,
    from: &NamespacePath,
    to: &NamespacePath,
    commit: &Commit,
) -> CommandOutput {
    context.output(CommandData::PathMove {
        from: render_target(context.namespace(), from.absolute_path()),
        to: render_target(context.namespace(), to.absolute_path()),
        committed_seq: commit.committed_seq,
        commit_id: commit.commit_id.clone(),
    })
}
