//! Filesystem commands: ls, stat, get, put, mkdir, rm, mv, cp, revisions,
//! and grep.

use super::context::{
    default_remote_put_path, destination_path_for_get, destination_user_path, directory_intent,
    fail, namespace_path, parse_public_ordinal_arg, parse_user_path, render_target,
    resolve_command_context, resolve_mutation_context, CommandContext, UndeleteHint,
};
use super::output::{
    CommandData, CommandFailure, CommandOutput, ListingHeadDrift, ListingHeadObservation,
    TrashListing,
};
use super::pagination::{write_jsonl_page, PagePlan};
use super::partial::{self, PartialDownload, PartialMeta};
use super::recursive;
use crate::args::{
    CommandKind, FilesystemAnnotateArgs, FilesystemCatArgs, FilesystemGetArgs, FilesystemGrepArgs,
    FilesystemLsArgs, FilesystemMkdirArgs, FilesystemPutArgs, FilesystemRestoreArgs,
    FilesystemRevisionsArgs, FilesystemRmArgs, FilesystemStatArgs, FilesystemTransferArgs,
    FilesystemUndeleteArgs, PaginationArgs, RuntimeBehavior, TrashArgs,
};
use crate::backend::FileDownload;
use crate::config::ConfigLocation;
use crate::error::CliError;
use crate::payload::{read_whole_file, LocalPayload, STDIN_PATH};
use crate::progress::{ProgressOp, ProgressReporter};
use crate::uploads::{SourceIdentity, UploadJournal};
use loonfs_api::v0::UploadSessionStatus;
use loonfs_api::{
    AbsolutePath, ActorRef, AttributeKey, AttributeRevisionNo, AttributeValue, ChangeSeq, CommitId,
    CommitResponse, DeleteDirectoryBehavior, DestinationBehavior, ErrorCode, InodeKind,
    ListPathEntriesResponse, NamespaceId, RevisionNo,
};
use loonfs_client::{
    CommitOptions, CreateDirectoryOptions, DeleteOptions, NamespacePath, PutFileOptions,
    UpdateAttributesOptions,
};
use std::collections::BTreeMap;
use std::fs;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

// --- filesystem ---

fn parse_commit_id_arg(commit_id: Option<&str>) -> Result<Option<CommitId>, CliError> {
    commit_id
        .map(|value| {
            CommitId::parse(value).map_err(|error| {
                CliError::invalid_input(format!("invalid --commit-id: {error}"))
                    .with_param("--commit-id")
            })
        })
        .transpose()
}

fn commit_options(
    actor: &ActorRef,
    commit_id: Option<CommitId>,
    message: Option<String>,
) -> CommitOptions {
    CommitOptions {
        actor: actor.clone(),
        commit_id,
        message,
    }
}

struct FollowedPathEntryPages {
    namespace_id: NamespaceId,
    path: AbsolutePath,
    head_seq: ChangeSeq,
    head_drift: Option<ListingHeadDrift>,
    next_cursor: Option<String>,
}

async fn follow_path_entry_pages(
    context: &CommandContext,
    kind: CommandKind,
    spec: &NamespacePath,
    pagination: &PaginationArgs,
    cursor: Option<&str>,
    mut visit: impl FnMut(Vec<loonfs_api::AuthoritativePathEntry>) -> Result<(), CliError>,
) -> Result<FollowedPathEntryPages, CommandFailure> {
    let mut plan = PagePlan::new(pagination);
    let mut cursor = cursor.map(ToOwned::to_owned);
    let mut heads = ListingHeadObservation::default();
    loop {
        let page = context
            .target
            .list_path_entries_page(spec, plan.request_size(), cursor.as_deref())
            .await
            .map_err(|error| context.fail(kind, error))?;
        let ListPathEntriesResponse {
            namespace_id,
            path: absolute_path,
            head_seq,
            entries,
            next_cursor,
        } = page;
        heads.observe(head_seq);
        plan.record(entries.len());
        visit(entries).map_err(|error| context.fail(kind, error))?;
        cursor = next_cursor;

        if !plan.should_continue(cursor.is_some()) {
            return Ok(FollowedPathEntryPages {
                namespace_id,
                path: absolute_path,
                head_seq: heads
                    .last()
                    .expect("a listing observes the page it just received"),
                head_drift: heads.drift(),
                next_cursor: cursor,
            });
        }
    }
}

pub(crate) async fn run_filesystem_ls(
    kind: CommandKind,
    config_path: &Path,
    args: FilesystemLsArgs,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let allow_root = true;
    let spec = namespace_path(
        &context.namespace,
        args.path.as_deref().unwrap_or("/"),
        allow_root,
    )
    .map_err(|error| context.fail(kind, error))?;
    let streams_pages = args.pagination.jsonl || (args.pagination.all && !runtime.json);
    if streams_pages {
        let stdout = io::stdout();
        let mut stdout = BufWriter::with_capacity(64 * 1024, stdout.lock());
        let followed = follow_path_entry_pages(
            &context,
            kind,
            &spec,
            &args.pagination,
            args.cursor.as_deref(),
            |entries| {
                write_path_entries_page(&mut stdout, &entries, args.pagination.jsonl)
                    .map_err(CliError::io)
            },
        )
        .await?;
        // Write head-drift warnings to stderr so streamed stdout contains
        // only entries.
        if let Some(drift) = followed.head_drift {
            crate::render::write_listing_drift_warning(&drift);
        }
        return Ok(CommandOutput {
            kind,
            profile: Some(context.profile_name),
            mode: Some(context.mode),
            data: CommandData::StreamedToStdout,
        });
    }

    let mut entries = Vec::new();
    let followed = follow_path_entry_pages(
        &context,
        kind,
        &spec,
        &args.pagination,
        args.cursor.as_deref(),
        |page| {
            entries.extend(page);
            Ok(())
        },
    )
    .await?;
    if !runtime.json {
        if let Some(drift) = followed.head_drift.as_ref() {
            crate::render::write_listing_drift_warning(drift);
        }
    }
    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::PathEntries {
            namespace_id: followed.namespace_id,
            path: followed.path,
            head_seq: followed.head_seq,
            head_drift: followed.head_drift,
            entries,
            next_cursor: followed.next_cursor,
        },
    })
}

pub(crate) async fn run_filesystem_stat(
    kind: CommandKind,
    config_path: &Path,
    args: FilesystemStatArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let entry = match args.inode {
        Some(inode_id) => {
            context
                .target
                .stat_inode(&context.namespace, inode_id)
                .await
        }
        None => {
            let path = args
                .path
                .as_deref()
                .expect("clap requires either path or --inode");
            let allow_root = true;
            let spec = namespace_path(&context.namespace, path, allow_root)
                .map_err(|error| context.fail(kind, error))?;
            context.target.stat_path(&spec).await
        }
    }
    .map_err(|error| context.fail(kind, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::PathEntry(entry),
    })
}

/// One `--set key=value` argument. The key ends at the first `=` so a value
/// may contain more of them, and a missing `=` names the flag rather than
/// guessing what the caller meant.
fn parse_attribute_assignment(argument: &str) -> Result<(AttributeKey, AttributeValue), CliError> {
    let Some((key, value)) = argument.split_once('=') else {
        return Err(CliError::invalid_input(format!(
            "invalid --set `{argument}`: expected key=value"
        ))
        .with_param("--set"));
    };
    Ok((
        parse_attribute_key_arg("--set", key)?,
        AttributeValue::parse(value).map_err(|error| {
            CliError::invalid_input(format!("invalid --set value: {error}")).with_param("--set")
        })?,
    ))
}

fn parse_attribute_key_arg(flag: &str, key: &str) -> Result<AttributeKey, CliError> {
    AttributeKey::parse(key).map_err(|error| {
        CliError::invalid_input(format!("invalid {flag} key: {error}")).with_param(flag)
    })
}

/// The `--attributes-json` form of the update: one object carrying the same
/// `set` and `remove` the flags build, with values in the wire encoding.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct AttributeUpdateJson {
    #[serde(default)]
    set: BTreeMap<AttributeKey, AttributeValue>,
    #[serde(default)]
    remove: Vec<AttributeKey>,
}

fn update_attributes_options(
    args: &FilesystemAnnotateArgs,
    actor: &ActorRef,
) -> Result<UpdateAttributesOptions, CliError> {
    let commit_id = parse_commit_id_arg(args.commit_id.as_deref())?;
    let (set, remove) = match args.attributes_json.as_deref() {
        Some(document) => {
            let update: AttributeUpdateJson = serde_json::from_str(document).map_err(|error| {
                CliError::invalid_input(format!(
                    "invalid --attributes-json attribute update: {error}"
                ))
                .with_param("--attributes-json")
            })?;
            (update.set, update.remove)
        }
        None => (
            args.sets
                .iter()
                .map(|assignment| parse_attribute_assignment(assignment))
                .collect::<Result<BTreeMap<_, _>, _>>()?,
            args.removes
                .iter()
                .map(|key| parse_attribute_key_arg("--remove", key))
                .collect::<Result<Vec<_>, _>>()?,
        ),
    };
    Ok(UpdateAttributesOptions {
        set,
        remove,
        commit: commit_options(actor, commit_id, args.message.clone()),
        expected_inode_id: args.expected_inode_id,
        expected_attributes_revision_no: args
            .expected_attributes_revision
            .map(|value| {
                parse_public_ordinal_arg(
                    "--expected-attributes-revision",
                    value,
                    AttributeRevisionNo::parse,
                )
            })
            .transpose()?,
    })
}

pub(crate) async fn run_filesystem_annotate(
    kind: CommandKind,
    config_path: &Path,
    args: FilesystemAnnotateArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_mutation_context(kind, config_path, &args.target, &args.actor).await?;
    let allow_root = true;
    let spec = namespace_path(&context.namespace, &args.path, allow_root)
        .map_err(|error| context.fail(kind, error))?;
    let options = update_attributes_options(&args, &context.actor)
        .map_err(|error| context.fail(kind, error))?;
    let result = context
        .target
        .update_attributes(&spec, &options)
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
            recovery_command: None,
        },
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
        cursor: args.cursor.clone(),
        limit: None,
        allow_stale: args.allow_stale,
        allow_scan: args.allow_scan,
    };
    let mut matches = Vec::new();
    let mut tail_scanned = true;
    let mut plan = PagePlan::new(&args.pagination);
    let stdout = io::stdout();
    let mut stdout = BufWriter::with_capacity(64 * 1024, stdout.lock());
    let (namespace_id, head_seq, built_through_seq, next_cursor) = loop {
        request.limit = plan.request_size();
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
        plan.record(response.matches.len());
        if args.pagination.jsonl {
            write_jsonl_page(&mut stdout, &response.matches)
                .map_err(CliError::io)
                .map_err(|error| context.fail(kind, error))?;
        } else {
            matches.extend(response.matches);
        }
        tail_scanned &= response.tail_scanned;
        let next_cursor = response.next_cursor;
        if !plan.should_continue(next_cursor.is_some()) {
            break (snapshot.0, snapshot.1, snapshot.2, next_cursor);
        }
        request.cursor.clone_from(&next_cursor);
    };
    if args.pagination.jsonl {
        return Ok(CommandOutput {
            kind,
            profile: Some(context.profile_name),
            mode: Some(context.mode),
            data: CommandData::StreamedToStdout,
        });
    }
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
            truncated: next_cursor.is_some(),
            next_cursor,
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
    let revision_no = args
        .revision
        .map(|value| parse_public_ordinal_arg("--revision", value, RevisionNo::parse))
        .transpose()
        .map_err(|error| context.fail(kind, error))?;
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
            CliError::json_not_supported(),
        ));
    }

    let allow_root = args.recursive;
    let spec = namespace_path(&context.namespace, &args.remote_path, allow_root)
        .map_err(|error| context.fail(kind, error))?;
    let revision_no = args
        .revision
        .map(|value| parse_public_ordinal_arg("--revision", value, RevisionNo::parse))
        .transpose()
        .map_err(|error| context.fail(kind, error))?;
    let entry = context
        .target
        .stat_path_without_attributes(&spec)
        .await
        .map_err(|error| context.fail(kind, error))?;
    if args.recursive {
        if entry.inode_kind() != InodeKind::Directory {
            return Err(context.fail(
                kind,
                CliError::invalid_input(format!(
                    "`{}` is not a directory; drop -r to download one file",
                    spec.absolute_path()
                ))
                .with_param("-r"),
            ));
        }
        if args.revision.is_some() {
            return Err(context.fail(
                kind,
                CliError::invalid_input("--revision applies to one file, not a tree")
                    .with_param("--revision"),
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
    if entry.inode_kind() == InodeKind::Directory {
        return Err(context.fail(
            kind,
            CliError::invalid_input(format!(
                "`{}` is a directory; use `loonfs get -r` to download the tree",
                spec.absolute_path()
            )),
        ));
    }

    if args.local_destination.as_deref() == Some("-") {
        // No progress and no resume: standard output is carrying the file,
        // and bytes already piped onward are somewhere this CLI cannot see.
        let mut download = context
            .target
            .open_file_download(&spec, revision_no, entry.size_bytes(), 0)
            .await
            .map_err(|error| context.fail(kind, error))?;
        stream_download_to_stdout(&mut download)
            .await
            .map_err(|error| context.fail(kind, error))?;
        return Ok(CommandOutput {
            kind,
            profile: Some(context.profile_name),
            mode: Some(context.mode),
            data: CommandData::StreamedToStdout,
        });
    }

    let derived_name = args.local_destination.is_none();
    let destination = destination_path_for_get(
        spec.absolute_path().as_str(),
        args.local_destination.as_deref(),
    )
    .map_err(|error| context.fail(kind, error))?;
    // Where a download starts is decided before it is opened: the bytes an
    // interrupted run left are named after this destination, and how many of
    // them still describe the content resolved just now is how far in this
    // one begins. A file with no content reference to compare against — one
    // this build cannot identify — starts over.
    let meta = entry
        .content_ref()
        .map(|content_ref| PartialMeta::describe(content_ref, revision_no));
    let start_offset = meta
        .as_ref()
        .map_or(0, |meta| partial::resumable_bytes(&destination, meta));
    let mut download = context
        .target
        .open_file_download(&spec, revision_no, entry.size_bytes(), start_offset)
        .await
        .map_err(|error| context.fail(kind, error))?;

    let progress = Arc::new(ProgressReporter::new(
        runtime,
        ProgressOp::Get,
        spec.absolute_path().as_str(),
    ));
    progress.expect(entry.size_bytes(), Some(1));
    progress.file_started(spec.absolute_path().as_str(), entry.size_bytes());
    // The local working copy is the one thing this CLI touches that has no
    // revision history behind it, so clobbering it is opt-in.
    // `persist_noclobber` closes the race between checking and installing
    // the completed partial file.
    let written = stream_download_to_file(
        &mut download,
        &destination,
        meta.as_ref(),
        args.force,
        derived_name,
        &progress,
    )
    .await;
    if let Ok(bytes_written) = &written {
        progress.file_finished(spec.absolute_path().as_str(), *bytes_written);
    }
    progress.finish();
    let bytes_written = written.map_err(|error| context.fail(kind, error))?;
    let data = CommandData::FileTransfer {
        target: render_target(&context.namespace, spec.absolute_path()),
        destination: destination.display().to_string(),
        bytes_written,
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
pub(super) async fn stream_download_to_file(
    download: &mut FileDownload,
    destination: &Path,
    meta: Option<&PartialMeta>,
    force: bool,
    derived_name: bool,
    progress: &ProgressReporter,
) -> Result<u64, CliError> {
    let resumed_from = download.resumed_from();
    let mut partial = PartialDownload::open(destination, meta, resumed_from)
        .map_err(|error| local_open_error(destination, error, force, derived_name))?;
    partial.fold_into(download, |error| {
        local_destination_error(destination, error, force, derived_name)
    })?;
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
            .map_err(|error| local_destination_error(destination, error, force, derived_name))?;
        bytes_written += chunk.len() as u64;
        progress.advance(chunk.len() as u64);
    }
    partial
        .install(destination, force)
        .map_err(|error| local_destination_error(destination, error, force, derived_name))?;
    Ok(bytes_written)
}

/// Writes one listing page and makes it visible before the next fetch.
fn write_path_entries_page(
    stdout: &mut impl Write,
    entries: &[loonfs_api::AuthoritativePathEntry],
    jsonl: bool,
) -> io::Result<()> {
    for entry in entries {
        if jsonl {
            serde_json::to_writer(&mut *stdout, entry).map_err(io::Error::other)?;
            stdout.write_all(b"\n")?;
        } else {
            writeln!(stdout, "{}", crate::render::human_path_entry(entry))?;
        }
    }
    stdout.flush()
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
/// wrong thing: it names the partial file this CLI picked, which the caller
/// never asked for and cannot act on. The parent they have to create is what
/// the message says instead.
fn local_open_error(
    destination: &Path,
    error: std::io::Error,
    force: bool,
    derived_name: bool,
) -> CliError {
    if error.kind() == std::io::ErrorKind::NotFound {
        let parent_error = std::io::Error::new(
            error.kind(),
            format!(
                "parent directory `{}` does not exist",
                partial::parent_of(destination).display()
            ),
        );
        return CliError::io_for_path(destination, parent_error);
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
    location: &ConfigLocation,
    args: TrashArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &location.path, &args.target).await?;
    let mut plan = PagePlan::new(&args.pagination);
    let mut cursor = args.cursor.clone();
    let mut response: Option<loonfs_api::ListTrashResponse> = None;
    let stdout = io::stdout();
    let mut stdout = BufWriter::with_capacity(64 * 1024, stdout.lock());
    loop {
        let page = context
            .target
            .list_trash(&context.namespace, plan.request_size(), cursor.as_deref())
            .await
            .map_err(|error| context.fail(kind, error))?;
        plan.record(page.entries.len());
        cursor = page.next_cursor.clone();
        if args.pagination.jsonl {
            write_jsonl_page(&mut stdout, &page.entries)
                .map_err(CliError::io)
                .map_err(|error| context.fail(kind, error))?;
        } else if let Some(response) = response.as_mut() {
            response.head_seq = page.head_seq;
            response.entries.extend(page.entries);
            response.next_cursor = page.next_cursor;
        } else {
            response = Some(page);
        }
        if !plan.should_continue(cursor.is_some()) {
            break;
        }
    }
    if args.pagination.jsonl {
        return Ok(CommandOutput {
            kind,
            profile: Some(context.profile_name),
            mode: Some(context.mode),
            data: CommandData::StreamedToStdout,
        });
    }
    let response = response.expect("trash loop should fetch at least one page");
    let hint = UndeleteHint::new(&context, location, args.target.profile.profile.is_some());
    // An entry that recorded its binding restores in place with no
    // destination in the command; only a legacy entry that recorded none
    // still needs the caller to supply one.
    let recovery_commands = response
        .entries
        .iter()
        .map(|entry| {
            hint.command(
                entry.display_name.is_some(),
                entry.inode_id,
                entry.deletion_seq,
            )
        })
        .collect();
    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::Trash(TrashListing {
            response,
            recovery_commands,
        }),
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
    let mut plan = PagePlan::new(&args.pagination);
    let mut cursor = args.cursor.clone();
    let mut revisions = Vec::new();
    let stdout = io::stdout();
    let mut stdout = BufWriter::with_capacity(64 * 1024, stdout.lock());
    loop {
        let page = context
            .target
            .list_file_revisions_page(&spec, plan.request_size(), cursor.as_deref())
            .await
            .map_err(|error| context.fail(kind, error))?;
        plan.record(page.revisions.len());
        cursor = page.next_cursor;
        if args.pagination.jsonl {
            write_jsonl_page(&mut stdout, &page.revisions)
                .map_err(CliError::io)
                .map_err(|error| context.fail(kind, error))?;
        } else {
            revisions.extend(page.revisions);
        }
        if !plan.should_continue(cursor.is_some()) {
            break;
        }
    }
    if args.pagination.jsonl {
        return Ok(CommandOutput {
            kind,
            profile: Some(context.profile_name),
            mode: Some(context.mode),
            data: CommandData::StreamedToStdout,
        });
    }

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::FileRevisions {
            target: render_target(&context.namespace, spec.absolute_path()),
            revisions,
            next_cursor: cursor,
        },
    })
}

pub(crate) async fn run_filesystem_put(
    kind: CommandKind,
    config_path: &Path,
    args: FilesystemPutArgs,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_mutation_context(kind, config_path, &args.target, &args.actor).await?;
    let local_path = PathBuf::from(&args.local_path);
    if local_path == Path::new(STDIN_PATH) {
        return run_filesystem_put_stdin(kind, args, context, runtime).await;
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
                ))
                .with_param("-r"),
            ));
        }
        if args.commit_id.is_some() {
            return Err(context.fail(
                kind,
                CliError::invalid_input(
                    "--commit-id names one commit; a recursive upload makes one commit per file",
                )
                .with_param("--commit-id"),
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
    let options =
        put_file_options(&args, &context.actor).map_err(|error| context.fail(kind, error))?;
    commit_put(
        kind,
        &context,
        &spec,
        &payload,
        &options,
        runtime,
        Some(metadata.len()),
    )
    .await
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
    runtime: RuntimeBehavior,
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
    let options =
        put_file_options(&args, &context.actor).map_err(|error| context.fail(kind, error))?;
    // A pipe cannot say how long it is, so there is a byte count but never a
    // total, a percentage, or an estimate.
    commit_put(
        kind,
        &context,
        &spec,
        &LocalPayload::Stdin,
        &options,
        runtime,
        None,
    )
    .await
}

/// Writes one payload and renders what the commit did.
async fn commit_put(
    kind: CommandKind,
    context: &CommandContext,
    spec: &NamespacePath,
    payload: &LocalPayload,
    options: &PutFileOptions,
    runtime: RuntimeBehavior,
    size_bytes: Option<u64>,
) -> Result<CommandOutput, CommandFailure> {
    let progress = Arc::new(ProgressReporter::new(
        runtime,
        ProgressOp::Put,
        spec.absolute_path().as_str(),
    ));
    progress.expect(size_bytes, Some(1));
    progress.file_started(spec.absolute_path().as_str(), size_bytes);
    let result = put_payload(context, spec, payload, options, &progress).await;
    if result.is_ok() {
        let moved = progress.bytes_done();
        progress.file_finished(spec.absolute_path().as_str(), moved);
    }
    progress.finish();
    let result = result.map_err(|error| context.fail(kind, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name.clone()),
        mode: Some(context.mode.clone()),
        data: CommandData::FileMutation {
            target: render_target(&context.namespace, spec.absolute_path()),
            committed_seq: result.committed_seq,
            commit_id: result.commit_id,
            inode_id: None,
            recovery_command: None,
        },
    })
}

/// Uploads one file and commits it at `spec`.
///
/// Small payloads are buffered. Large payloads and streams are uploaded in
/// chunks with bounded memory. Multipart uploads from files can resume after
/// an interruption.
///
/// Recursive uploads share `progress` across their files.
pub(super) async fn put_payload(
    context: &CommandContext,
    spec: &NamespacePath,
    payload: &LocalPayload,
    options: &PutFileOptions,
    progress: &Arc<ProgressReporter>,
) -> Result<CommitResponse, CliError> {
    // Resume only multipart uploads backed by a file that can be reopened.
    // Streams cannot be read again after an interruption.
    let journal = payload
        .resumable_source()
        .and_then(|local_path| resume_journal(context, spec, local_path));
    if let Some(journal) = journal.as_ref() {
        if let Some(committed) =
            commit_a_finished_upload(context, spec, options, journal, progress).await?
        {
            return Ok(committed);
        }
    }
    let result = match payload.holdable_file() {
        // A payload small enough to hold travels as one request, so there is
        // no midpoint to report: it is read, and then the commit is all
        // that is left.
        Some(path) => {
            let bytes = read_whole_file(path).await?;
            progress.advance(bytes.len() as u64);
            progress.phase("committing");
            context.target.put_file_bytes(spec, &bytes, options).await
        }
        None => {
            context
                .target
                .put_file_stream(spec, payload, options, progress, journal.as_ref())
                .await
        }
    };
    if result.is_ok() {
        // The record exists to survive an interruption, and this upload was
        // not interrupted.
        if let Some(journal) = journal.as_ref() {
            journal.forget();
        }
    }
    result.map_err(CliError::from)
}

/// The record an interrupted upload of this payload would have left, or
/// nothing when there is nowhere to keep one.
fn resume_journal(
    context: &CommandContext,
    spec: &NamespacePath,
    local_path: &Path,
) -> Option<UploadJournal> {
    let source = SourceIdentity::of(local_path).ok()?;
    UploadJournal::for_upload(
        &context.profile_name,
        context.namespace.as_str(),
        spec.absolute_path().as_str(),
        local_path,
        source,
    )
}

/// Commits an upload whose bytes all landed before it was interrupted,
/// without sending any of them again.
///
/// An interruption between the last part and the commit leaves a session
/// the server already completed: the object is assembled and admitted, and
/// only the commit is missing. Asking the session what became of it is one
/// round trip and saves the whole transfer. Any other answer — still open,
/// aborted, or gone — leaves this to the ordinary upload, which the
/// recorded parts make cheap anyway.
async fn commit_a_finished_upload(
    context: &CommandContext,
    spec: &NamespacePath,
    options: &PutFileOptions,
    journal: &UploadJournal,
    progress: &ProgressReporter,
) -> Result<Option<CommitResponse>, CliError> {
    let Some(resume) = journal.resume() else {
        return Ok(None);
    };
    let Ok(status) = context
        .target
        .read_upload_status(&context.namespace, &resume.upload_id)
        .await
    else {
        return Ok(None);
    };
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
        .target
        .commit_completed_upload(spec, content_ref, content_token, options)
        .await;
    if result.is_ok() {
        journal.forget();
    }
    Ok(Some(result?))
}

fn put_file_options(
    args: &FilesystemPutArgs,
    actor: &ActorRef,
) -> Result<PutFileOptions, CliError> {
    let commit_id = parse_commit_id_arg(args.commit_id.as_deref())?;
    let expected_revision_no = args
        .expected_revision
        .map(|value| parse_public_ordinal_arg("--expected-revision", value, RevisionNo::parse))
        .transpose()?;
    // The revision guard is a stronger replace statement, so it implies
    // --force rather than demanding both flags.
    let behavior = if args.force || expected_revision_no.is_some() {
        DestinationBehavior::Replace
    } else {
        DestinationBehavior::NoReplace
    };
    Ok(PutFileOptions {
        behavior,
        commit: commit_options(actor, commit_id, args.message.clone()),
        expected_revision_no,
    })
}

pub(crate) async fn run_filesystem_rm(
    kind: CommandKind,
    location: &ConfigLocation,
    args: FilesystemRmArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_mutation_context(kind, &location.path, &args.target, &args.actor).await?;
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
        .stat_path_without_attributes(&spec)
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
        commit: commit_options(&context.actor, commit_id, args.message.clone()),
    };
    let result = context
        .target
        .delete_path(&spec, &options)
        .await
        .map_err(|error| context.fail(kind, error))?;

    // A delete resolved through a path always records its binding, so the
    // printed command restores in place with no destination — and keeps
    // working even if the enclosing directories are renamed before the
    // paste.
    let recovery_command = UndeleteHint::new(
        &context,
        location,
        args.target.profile.profile.is_some(),
    )
    .command(true, deleted_inode, result.committed_seq);

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::FileMutation {
            target: render_target(&context.namespace, spec.absolute_path()),
            committed_seq: result.committed_seq,
            commit_id: result.commit_id,
            inode_id: Some(deleted_inode),
            recovery_command: Some(recovery_command),
        },
    })
}

pub(crate) async fn run_filesystem_restore(
    kind: CommandKind,
    config_path: &Path,
    args: FilesystemRestoreArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_mutation_context(kind, config_path, &args.target, &args.actor).await?;
    let allow_root = false;
    let spec = namespace_path(&context.namespace, &args.path, allow_root)
        .map_err(|error| context.fail(kind, error))?;
    let commit_id = parse_commit_id_arg(args.commit_id.as_deref())
        .map_err(|error| context.fail(kind, error))?;
    let revision_no = parse_public_ordinal_arg("--revision", args.revision, RevisionNo::parse)
        .map_err(|error| context.fail(kind, error))?;
    let result = context
        .target
        .restore_file_revision(
            &spec,
            revision_no,
            &loonfs_client::RestoreRevisionOptions {
                commit: commit_options(&context.actor, commit_id, args.message.clone()),
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
            recovery_command: None,
        },
    })
}

pub(crate) async fn run_filesystem_undelete(
    kind: CommandKind,
    config_path: &Path,
    args: FilesystemUndeleteArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_mutation_context(kind, config_path, &args.target, &args.actor).await?;
    let allow_root = false;
    // An absent path restores in place; the destination is then the parent
    // and name the deletion recorded, which no path here could name better.
    let spec = args
        .path
        .as_deref()
        .map(|path| namespace_path(&context.namespace, path, allow_root))
        .transpose()
        .map_err(|error| context.fail(kind, error))?;
    let commit_id = parse_commit_id_arg(args.commit_id.as_deref())
        .map_err(|error| context.fail(kind, error))?;
    let deletion_seq =
        parse_public_ordinal_arg("--deletion-seq", args.deletion_seq, ChangeSeq::parse)
            .map_err(|error| context.fail(kind, error))?;
    let result = context
        .target
        .undelete(
            &context.namespace,
            args.inode,
            deletion_seq,
            spec.as_ref().map(|spec| spec.absolute_path()),
            &loonfs_client::UndeleteOptions {
                commit: commit_options(&context.actor, commit_id, args.message.clone()),
            },
        )
        .await
        .map_err(|error| context.fail(kind, error))?;

    let target = match spec.as_ref() {
        Some(spec) => render_target(&context.namespace, spec.absolute_path()),
        None => format!("{}:(restored in place)", context.namespace),
    };
    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::FileMutation {
            target,
            committed_seq: result.committed_seq,
            commit_id: result.commit_id,
            inode_id: Some(args.inode),
            recovery_command: None,
        },
    })
}

pub(crate) async fn run_filesystem_mkdir(
    kind: CommandKind,
    config_path: &Path,
    args: FilesystemMkdirArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_mutation_context(kind, config_path, &args.target, &args.actor).await?;
    let allow_root = false;
    let spec = namespace_path(&context.namespace, &args.path, allow_root)
        .map_err(|error| context.fail(kind, error))?;
    let commit_id = parse_commit_id_arg(args.commit_id.as_deref())
        .map_err(|error| context.fail(kind, error))?;
    let options = CreateDirectoryOptions {
        parents: args.parents,
        commit: commit_options(&context.actor, commit_id, args.message.clone()),
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
                .stat_path_without_attributes(&spec)
                .await
                .map_err(|_| context.fail(kind, error.clone()))?;
            if existing.inode_kind() != InodeKind::Directory {
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
            recovery_command: None,
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
    let Ok(existing) = context.target.stat_path_without_attributes(&named).await else {
        // Absent, or unreadable for a reason the transfer itself will
        // report: either way this is not a directory to land inside.
        return Ok(named);
    };
    if existing.inode_kind() != InodeKind::Directory {
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
    let context = resolve_mutation_context(kind, config_path, &args.target, &args.actor).await?;
    if args.recursive && transfer_kind == TransferKind::Move {
        return Err(context.fail(
            kind,
            CliError::invalid_input("mv moves a directory in one commit; -r is not needed")
                .with_param("-r"),
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
    let behavior = if args.force {
        DestinationBehavior::Replace
    } else {
        DestinationBehavior::NoReplace
    };
    let result = if transfer_kind == TransferKind::Copy {
        let entry = context
            .target
            .stat_path_without_attributes(&from)
            .await
            .map_err(|error| context.fail(kind, error))?;
        if args.recursive {
            if entry.inode_kind() != InodeKind::Directory {
                return Err(context.fail(
                    kind,
                    CliError::invalid_input(format!(
                        "`{}` is not a directory; drop -r to copy one file",
                        from.absolute_path()
                    ))
                    .with_param("-r"),
                ));
            }
            if args.commit_id.is_some() {
                return Err(context.fail(
                    kind,
                    CliError::invalid_input(
                        "--commit-id names one commit; a recursive copy makes one commit per item",
                    )
                    .with_param("--commit-id"),
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
        if entry.inode_kind() == InodeKind::Directory {
            return Err(context.fail(
                kind,
                CliError::invalid_input(format!(
                    "`{}` is a directory; use `loonfs cp -r` to copy the tree",
                    from.absolute_path()
                )),
            ));
        }
        context
            .target
            .copy_path(
                &from,
                &to,
                &loonfs_client::CopyOptions {
                    behavior,
                    commit: commit_options(&context.actor, commit_id, args.message.clone()),
                },
            )
            .await
    } else {
        context
            .target
            .move_path(
                &from,
                &to,
                &loonfs_client::MoveOptions {
                    behavior,
                    commit: commit_options(&context.actor, commit_id, args.message.clone()),
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
