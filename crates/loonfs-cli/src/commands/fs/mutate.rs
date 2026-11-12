//! The `rm`, `restore`, and `mkdir` commands, and the commit and
//! destination options that every mutating command shares.

use crate::args::{CommitArgs, FilesystemMkdirArgs, FilesystemRestoreArgs, FilesystemRmArgs};
use crate::commands::context::{
    create_directory_tolerating_existing, namespace_path, render_target, shell_quote, Invocation,
    NamespaceContext, ProfileSource, RemoteDirectoryOutcome, RestoreDeletedHint, RootPath,
};
use crate::commands::output::{CommandData, CommandFailure, CommandOutput};
use crate::error::{CliError, Result};
use loonfs_client::{
    CommitOptions, CreateDirectoryOptions, DeleteOptions, RestoreDeletedDestination,
    RestoreDeletedOptions,
};
use loonfs_types::{
    CommitSeq, DeleteDirectoryBehavior, DestinationBehavior, ErrorCode, InodeId, RevisionNo,
};

pub(crate) fn commit_options(args: &CommitArgs) -> CommitOptions {
    CommitOptions {
        preconditions: Vec::new(),
        commit_id: args.commit_id.clone(),
        message: args.message.clone(),
    }
}

/// The destination behavior `--force` asks for.
pub(crate) fn destination_behavior(force: bool) -> DestinationBehavior {
    if force {
        DestinationBehavior::Replace
    } else {
        DestinationBehavior::NoReplace
    }
}

pub(crate) async fn run_filesystem_rm(
    invocation: &Invocation,
    args: FilesystemRmArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = NamespaceContext::resolve(invocation, args.actor.actor_id.as_ref()).await?;
    let root = RootPath::Refused;
    let spec = namespace_path(context.namespace(), "path", &args.path, root)
        .map_err(|error| context.fail(error))?;
    let commit = commit_options(&args.commit);
    // Resolve the inode before deleting: the id is half of the recovery
    // handle `loonfs restore` needs. The delete then includes it as an
    // expectation, so a rebinding racing this command fails the delete
    // instead of removing (and mis-reporting) a different inode.
    let deleted_inode = context
        .profile
        .target
        .stat_without_attributes(&spec)
        .await
        .map_err(|error| context.fail(error))?
        .inode_id;
    let behavior = if args.recursive {
        DeleteDirectoryBehavior::Recursive
    } else {
        DeleteDirectoryBehavior::NonRecursive
    };
    let options = DeleteOptions {
        behavior,
        expected_inode_id: Some(deleted_inode),
        commit,
    };
    let result = context
        .profile
        .target
        .client
        .delete_path_with_options(&spec, context.actor(), &options)
        .await
        .map_err(|error| {
            let error = CliError::from(error);
            context.fail(if args.recursive {
                error
            } else {
                rm_not_empty_hint(error, &args.path)
            })
        })?;

    // A delete resolved through a path always records its binding, so the
    // printed command restores in place with no destination — and keeps
    // working even if the enclosing directories are renamed before the
    // paste.
    let recovery_command = RestoreDeletedHint::new(
        &context,
        &invocation.config,
        ProfileSource::from_selectors(&invocation.selectors),
    )
    .command(deleted_inode, result.committed_seq);

    Ok(context.output(CommandData::FileMutation {
        human_target: None,
        target: render_target(context.namespace(), spec.absolute_path()),
        committed_seq: result.committed_seq,
        commit_id: result.commit_id,
        inode_id: Some(deleted_inode),
        recovery_command: Some(recovery_command),
    }))
}

/// Rewrites the `directory_not_empty` error of a non-recursive `rm` to say how
/// to delete the directory and everything under it.
fn rm_not_empty_hint(mut error: CliError, path: &str) -> CliError {
    if ErrorCode::parse(&error.code) == Some(ErrorCode::DirectoryNotEmpty) {
        error.message = format!(
            "directory `{path}` is not empty; use `loonfs rm -r {}` to delete it and everything under it",
            shell_quote(path)
        );
    }
    error
}

pub(crate) async fn run_filesystem_restore(
    invocation: &Invocation,
    args: FilesystemRestoreArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = NamespaceContext::resolve(invocation, args.actor.actor_id.as_ref()).await?;
    let commit = commit_options(&args.commit);
    match args.inode {
        Some(inode_id) => {
            let deletion_seq = args
                .deletion_seq
                .expect("clap requires `--deletion-seq` with `--inode`");
            restore_deleted(&context, inode_id, deletion_seq, args.to.as_deref(), commit).await
        }
        None => {
            let path = args.path.expect("clap requires a path without `--inode`");
            let revision_no = args
                .revision
                .expect("clap requires `--revision` without `--inode`");
            restore_revision(&context, &path, revision_no, &commit).await
        }
    }
}

async fn restore_revision(
    context: &NamespaceContext,
    path: &str,
    revision_no: RevisionNo,
    commit: &CommitOptions,
) -> Result<CommandOutput, CommandFailure> {
    let root = RootPath::Refused;
    let spec = namespace_path(context.namespace(), "path", path, root)
        .map_err(|error| context.fail(error))?;
    let result = context
        .profile
        .target
        .client
        .restore_revision_with_options(&spec, revision_no, context.actor(), commit)
        .await
        .map_err(|error| context.fail(error))?;

    Ok(context.output(CommandData::FileMutation {
        human_target: None,
        target: render_target(context.namespace(), spec.absolute_path()),
        committed_seq: result.committed_seq,
        commit_id: result.commit_id,
        inode_id: None,
        recovery_command: None,
    }))
}

async fn restore_deleted(
    context: &NamespaceContext,
    inode_id: InodeId,
    deletion_seq: CommitSeq,
    destination_path: Option<&str>,
    commit: CommitOptions,
) -> Result<CommandOutput, CommandFailure> {
    let root = RootPath::Refused;
    // An absent path restores in place; the destination is then the parent
    // and name the deletion recorded, which no path here could name better.
    let spec = destination_path
        .map(|path| namespace_path(context.namespace(), "--to", path, root))
        .transpose()
        .map_err(|error| context.fail(error))?;
    let destination = spec
        .as_ref()
        .map_or(RestoreDeletedDestination::Recorded, |spec| {
            RestoreDeletedDestination::Path(spec.absolute_path().clone())
        });
    let result = context
        .profile
        .target
        .client
        .restore_deleted_with_options(
            context.namespace(),
            inode_id,
            deletion_seq,
            context.actor(),
            &RestoreDeletedOptions {
                commit,
                destination,
            },
        )
        .await
        .map_err(|error| context.fail(error))?;

    let target = match spec.as_ref() {
        Some(spec) => render_target(context.namespace(), spec.absolute_path()),
        None => format!("{}:(restored in place)", context.namespace()),
    };
    Ok(context.output(CommandData::FileMutation {
        human_target: spec
            .is_none()
            .then(|| format!("in place in `{}`", context.namespace())),
        target,
        committed_seq: result.committed_seq,
        commit_id: result.commit_id,
        inode_id: Some(inode_id),
        recovery_command: None,
    }))
}

pub(crate) async fn run_filesystem_mkdir(
    invocation: &Invocation,
    args: FilesystemMkdirArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = NamespaceContext::resolve(invocation, args.actor.actor_id.as_ref()).await?;
    let root = RootPath::Refused;
    let spec = namespace_path(context.namespace(), "path", &args.path, root)
        .map_err(|error| context.fail(error))?;
    let commit = commit_options(&args.commit);
    let options = CreateDirectoryOptions {
        parents: args.parents,
        commit,
    };
    let outcome = if args.parents {
        create_directory_tolerating_existing(&context, &spec, &options).await
    } else {
        context
            .profile
            .target
            .client
            .create_directory_with_options(&spec, context.actor(), &options)
            .await
            .map(RemoteDirectoryOutcome::Created)
            .map_err(CliError::from)
    }
    .map_err(|error| context.fail(error))?;
    let result = match outcome {
        RemoteDirectoryOutcome::Created(result) => result,
        RemoteDirectoryOutcome::AlreadyExists { inode_id, head_seq } => {
            return Ok(context.output(CommandData::DirectoryAlreadyExists {
                target: render_target(context.namespace(), spec.absolute_path()),
                inode_id,
                head_seq,
            }));
        }
    };

    Ok(context.output(CommandData::FileMutation {
        human_target: None,
        target: render_target(context.namespace(), spec.absolute_path()),
        committed_seq: result.committed_seq,
        commit_id: result.commit_id,
        inode_id: None,
        recovery_command: None,
    }))
}
