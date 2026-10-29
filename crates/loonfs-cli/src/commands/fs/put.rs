//! The `put` and `append` commands, with the upload journal that resumes an
//! interrupted put.

use super::mutate::{commit_options, destination_behavior};
use crate::args::{FilesystemAppendArgs, FilesystemPutArgs, RuntimeBehavior};
use crate::commands::context::{
    default_remote_put_path, destination_user_path, parse_user_path_arg, render_target, Invocation,
    NamespaceContext, RootPath,
};
use crate::commands::output::{CommandData, CommandFailure, CommandOutput};
use crate::commands::recursive;
use crate::error::{CliError, Result};
use crate::payload::{LocalPayload, STDIN_PATH};
use crate::progress::{ProgressOp, ProgressReporter};
use crate::uploads::{SourceIdentity, UploadAttempt, UploadJournal};
use loonfs_client::{AppendFileOptions, NamespacePath, PutFileOptions};
use loonfs_types::format::wal::MAX_WAL_INLINE_CONTENT_BYTES;
use loonfs_types::{Commit, DestinationBehavior, UploadSessionStatus};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(crate) async fn run_filesystem_put(
    invocation: &Invocation,
    args: FilesystemPutArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = NamespaceContext::resolve(invocation, args.actor.actor_id.as_ref()).await?;
    let local_path = PathBuf::from(&args.local_path);
    if local_path == Path::new(STDIN_PATH) {
        return run_filesystem_put_stdin(invocation, args, context).await;
    }

    let metadata = fs::metadata(&local_path)
        .map_err(|error| context.fail(CliError::io_for_path(&local_path, error)))?;
    if args.recursive {
        if !metadata.is_dir() {
            return Err(context.fail(
                CliError::invalid_request(format!(
                    "`{}` is not a directory; drop -r to upload one file",
                    local_path.display()
                ))
                .with_param("-r"),
            ));
        }
        if args.commit.commit_id.is_some() {
            return Err(context.fail(
                CliError::invalid_request(
                    "`--commit-id` names one commit; a recursive upload makes one commit per file",
                )
                .with_param("--commit-id"),
            ));
        }
        let remote_root = match args.remote_path {
            Some(path) => parse_user_path_arg("remote_path", &path, RootPath::Allowed),
            None => default_remote_put_path(&local_path),
        }
        .map_err(|error| context.fail(error))?;
        return recursive::run_put_tree(
            &context,
            &local_path,
            &remote_root,
            destination_behavior(args.force),
            args.commit.message.clone(),
            invocation.behavior,
        )
        .await;
    }
    if metadata.is_dir() {
        return Err(context.fail(
            CliError::invalid_request(format!(
                "`{}` is a directory; use `loonfs put -r` to upload the tree",
                local_path.display()
            ))
            .with_param("local_path"),
        ));
    }

    let local_leaf = local_path.file_name().ok_or_else(|| {
        context.fail(
            CliError::invalid_request(format!(
                "unable to derive remote target from `{}`",
                local_path.display()
            ))
            .with_param("local_path"),
        )
    })?;
    let local_leaf = crate::commands::context::utf8_local_name(local_leaf)
        .map_err(|error| context.fail(error))?;
    let remote_path = match args.remote_path.as_deref() {
        // A trailing slash names the directory the file is written in — the
        // cp/rsync habit — while a plain path is the full destination.
        Some(path) => destination_user_path(
            "remote_path",
            "local_path",
            path,
            local_leaf,
            RootPath::Allowed,
        ),
        None => default_remote_put_path(&local_path),
    }
    .map_err(|error| context.fail(error))?;
    let spec = NamespacePath::new(context.namespace().clone(), remote_path);
    let payload = LocalPayload::file(&local_path, metadata.len());
    let options = put_file_options(&args).map_err(|error| context.fail(error))?;
    commit_put(&context, &spec, &payload, &options, invocation.behavior).await
}

/// `loonfs put - <remote>`: standard input, whose length is not knowable, so
/// it is always read once and never kept in memory.
///
/// The remote path has to be spelled out. Every other `put` derives a
/// default from the local file's name, and a pipe has none to derive from.
async fn run_filesystem_put_stdin(
    invocation: &Invocation,
    args: FilesystemPutArgs,
    context: NamespaceContext,
) -> Result<CommandOutput, CommandFailure> {
    if args.recursive {
        return Err(context.fail(
            CliError::invalid_request("`-` streams one file; a tree needs a directory")
                .with_param("-r"),
        ));
    }
    let Some(remote_path) = args.remote_path.as_deref() else {
        return Err(context.fail(
            CliError::invalid_request(
                "reading from `-` needs an explicit remote path; there is no local name to \
                 derive one from",
            )
            .with_param("remote_path"),
        ));
    };
    let remote_path = parse_user_path_arg("remote_path", remote_path, RootPath::Refused)
        .map_err(|error| context.fail(error))?;
    let spec = NamespacePath::new(context.namespace().clone(), remote_path);
    let options = put_file_options(&args).map_err(|error| context.fail(error))?;
    // A pipe cannot say how long it is, so there is a byte count but never a
    // total, a percentage, or an estimate.
    commit_put(
        &context,
        &spec,
        &LocalPayload::Stdin,
        &options,
        invocation.behavior,
    )
    .await
}

/// Writes one payload and renders what the commit did.
async fn commit_put(
    context: &NamespaceContext,
    spec: &NamespacePath,
    payload: &LocalPayload,
    options: &PutFileOptions,
    behavior: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let progress = Arc::new(ProgressReporter::new(
        behavior,
        ProgressOp::Put,
        spec.absolute_path().as_str(),
    ));
    let size_bytes = payload.size_bytes();
    progress.expect(size_bytes, Some(1));
    progress.file_started(spec.absolute_path().as_str(), size_bytes);
    let result = put_payload(context, spec, payload, options, &progress).await;
    if result.is_ok() {
        let moved = progress.bytes_done();
        progress.file_finished(spec.absolute_path().as_str(), moved);
    }
    progress.finish();
    let result = result.map_err(|error| context.fail(error))?;

    Ok(context.output(CommandData::FileMutation {
        human_target: None,
        target: render_target(context.namespace(), spec.absolute_path()),
        committed_seq: result.committed_seq,
        commit_id: result.commit_id,
        inode_id: None,
        recovery_command: None,
    }))
}

/// Uploads one file and commits it at `spec`.
///
/// File uploads retain their options and prepared request across interruptions.
///
/// Recursive uploads share `progress` across their files.
pub(crate) async fn put_payload(
    context: &NamespaceContext,
    spec: &NamespacePath,
    payload: &LocalPayload,
    options: &PutFileOptions,
    progress: &Arc<ProgressReporter>,
) -> Result<Commit> {
    let journal = payload
        .file_path()
        .map(|path| resume_journal(context, spec, path, options))
        .transpose()?;
    let actor = context.actor();
    let retained_options = journal.as_ref().map(UploadJournal::options);
    let options = retained_options.as_ref().unwrap_or(options);
    let result = async {
        if let Some(journal) = journal.as_ref() {
            if let Some(request) = journal.prepared_request() {
                progress.phase("committing");
                return journal
                    .replay(
                        &context.profile.target.client,
                        context.namespace(),
                        &request,
                        actor,
                    )
                    .await;
            }
            if let Some(committed) =
                commit_a_finished_upload(context, spec, options, journal, progress).await?
            {
                return Ok(committed);
            }
        }
        context
            .profile
            .target
            .put_file_stream(spec, payload, actor, options, progress, journal.as_ref())
            .await
    }
    .await;
    match journal {
        Some(journal) => journal.finish(result),
        None => result,
    }
}

/// Opens the journal for a resumable file upload.
fn resume_journal(
    context: &NamespaceContext,
    spec: &NamespacePath,
    local_path: &Path,
    options: &PutFileOptions,
) -> Result<UploadJournal> {
    let source =
        SourceIdentity::of(local_path).map_err(|error| CliError::io_for_path(local_path, error))?;
    let attempt = UploadAttempt::new(
        source,
        context.actor().clone(),
        options,
        context.profile.target.client.subject().cloned(),
    );
    UploadJournal::for_upload(
        &context.profile.profile_name,
        context.profile.target.journal_identity(),
        spec,
        local_path,
        attempt,
    )
    .map_err(CliError::io)
}

/// Commits an upload whose bytes were all uploaded before it was interrupted,
/// without sending any of them again.
///
/// An interruption between the last part and the commit leaves a session
/// the server already completed: the object is assembled and admitted, and
/// only the commit request is missing. A completed session supplies its content
/// and proof in one round trip. An open session resumes from recorded parts;
/// lookup and completion errors remain visible. Prepared requests skip this
/// lookup entirely.
async fn commit_a_finished_upload(
    context: &NamespaceContext,
    spec: &NamespacePath,
    options: &PutFileOptions,
    journal: &UploadJournal,
    progress: &ProgressReporter,
) -> Result<Option<Commit>> {
    let Some(resume) = journal.resume() else {
        return Ok(None);
    };
    let status = context
        .profile
        .target
        .client
        .get_upload(context.namespace(), &resume.upload_id)
        .await?;
    let UploadSessionStatus::Completed {
        content_ref,
        content_token,
        ..
    } = status.status
    else {
        return Ok(None);
    };
    progress.already_done(content_ref.size_bytes);
    progress.phase("committing");
    let result = context
        .profile
        .target
        .client
        .commit_completed_upload(
            spec,
            content_ref,
            content_token,
            context.actor(),
            options,
            Some(journal),
        )
        .await;
    Ok(Some(result?))
}

fn put_file_options(args: &FilesystemPutArgs) -> Result<PutFileOptions> {
    let expected_revision_no = args.expected_revision;
    let behavior =
        if args.force || args.expected_inode_id.is_some() || expected_revision_no.is_some() {
            DestinationBehavior::Replace
        } else {
            DestinationBehavior::NoReplace
        };
    Ok(PutFileOptions {
        behavior,
        commit: commit_options(&args.commit),
        expected_inode_id: args.expected_inode_id,
        expected_revision_no,
    })
}

pub(crate) async fn run_filesystem_append(
    invocation: &Invocation,
    args: FilesystemAppendArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = NamespaceContext::resolve(invocation, args.actor.actor_id.as_ref()).await?;
    let remote_path = parse_user_path_arg("remote_path", &args.remote_path, RootPath::Refused)
        .map_err(|error| context.fail(error))?;
    let spec = NamespacePath::new(context.namespace().clone(), remote_path);
    let options = append_file_options(&args).map_err(|error| context.fail(error))?;
    let bytes = read_append_content(&args.local_path)
        .await
        .map_err(|error| context.fail(error))?;
    let result = context
        .profile
        .target
        .client
        .append_file_with_options(&spec, &bytes, context.actor(), &options)
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

/// Reads the bytes one append includes. The read stops one byte past what an
/// append may include, so an oversized source never fills memory and the
/// server still returns `content_too_large`.
async fn read_append_content(local_path: &str) -> Result<Vec<u8>> {
    use tokio::io::AsyncReadExt as _;
    let limit = MAX_WAL_INLINE_CONTENT_BYTES as u64 + 1;
    let mut bytes = Vec::new();
    let read = if local_path == STDIN_PATH {
        tokio::io::stdin().take(limit).read_to_end(&mut bytes).await
    } else {
        match tokio::fs::File::open(local_path).await {
            Ok(file) => file.take(limit).read_to_end(&mut bytes).await,
            Err(error) => Err(error),
        }
    };
    read.map_err(|error| CliError::io_for_path(Path::new(local_path), error))?;
    Ok(bytes)
}

fn append_file_options(args: &FilesystemAppendArgs) -> Result<AppendFileOptions> {
    Ok(AppendFileOptions {
        commit: commit_options(&args.commit),
        expected_inode_id: args.expected_inode_id,
        expected_revision_no: args.expected_revision,
    })
}
