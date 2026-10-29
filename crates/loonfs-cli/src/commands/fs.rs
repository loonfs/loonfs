//! Filesystem commands: ls, stat, get, put, append, mkdir, rm, mv, cp,
//! revisions, and grep.

use super::context::{
    create_directory_tolerating_existing, default_remote_put_path, destination_path_for_get,
    destination_user_path, directory_intent, namespace_path, parse_user_path_arg, render_target,
    shell_quote, Invocation, NamespaceContext, ProfileSource, RemoteDirectoryOutcome, RootPath,
    UndeleteHint,
};
use super::download::{FileDownload, FileVersion};
use super::output::{
    CommandData, CommandFailure, CommandOutput, ListingHeadDrift, ListingHeadObservation,
    TrashListing,
};
use super::pagination::{collect_or_stream_pages, visit_pages, PagedListing};
use super::partial::{self, PartialDownload, PartialDownloadMetadata};
use super::recursive;
use crate::args::{
    CommitArgs, FilesystemAnnotateArgs, FilesystemAppendArgs, FilesystemCatArgs, FilesystemGetArgs,
    FilesystemGrepArgs, FilesystemLsArgs, FilesystemMkdirArgs, FilesystemPutArgs,
    FilesystemRestoreArgs, FilesystemRevisionsArgs, FilesystemRmArgs, FilesystemStatArgs,
    FilesystemTransferArgs, FilesystemUndeleteArgs, PaginationArgs, RuntimeBehavior, TrashArgs,
};
use crate::error::{CliError, Result};
use crate::payload::{LocalPayload, STDIN_PATH};
use crate::progress::{ProgressOp, ProgressReporter};
use crate::render::OutputFormat;
use crate::uploads::{SourceIdentity, UploadAttempt, UploadJournal};
use loonfs_client::{
    AppendFileOptions, AttributeChanges, CommitOptions, CreateDirectoryOptions, DeleteOptions,
    ListOptions, NamespacePath, PutFileOptions, ReadAt, StatOptions, UndeleteDestination,
    UndeleteOptions, UpdateAttributesOptions,
};
use loonfs_types::api::v0::UploadSessionStatus;
use loonfs_types::format::wal::MAX_WAL_INLINE_CONTENT_BYTES;
use loonfs_types::{
    AbsolutePath, AttributeKey, AttributeValue, ChangeSeq, Commit, DeleteDirectoryBehavior,
    DestinationBehavior, EntryInodeKind, ErrorCode, GrepResponse, ListPathEntriesResponse,
    NamespaceId,
};
use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(crate) fn commit_options(args: &CommitArgs) -> CommitOptions {
    CommitOptions {
        preconditions: Vec::new(),
        commit_id: args.commit_id.clone(),
        message: args.message.clone(),
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
    context: &NamespaceContext,
    spec: &NamespacePath,
    pagination: &PaginationArgs,
    at: &ReadAt,
    mut visit: impl FnMut(Vec<loonfs_types::PathEntry>) -> Result<()>,
) -> Result<FollowedPathEntryPages, CommandFailure> {
    let mut heads = ListingHeadObservation::default();
    let mut followed = None;
    visit_pages(
        context
            .profile
            .target
            .client
            .list_with_options(
                spec,
                &ListOptions {
                    at: at.clone(),
                    ..ListOptions::default()
                },
            )
            .cursor(pagination.cursor.clone()),
        &pagination.page_limits,
        |page: ListPathEntriesResponse| {
            heads.observe(page.head_seq);
            followed = Some(FollowedPathEntryPages {
                namespace_id: page.namespace_id,
                path: page.path,
                head_seq: page.head_seq,
                head_drift: heads.drift(),
                next_cursor: page.next_cursor,
            });
            visit(page.entries)
        },
    )
    .await
    .map_err(|error| context.fail(error))?;
    Ok(followed.expect("a listing should observe its first page"))
}

pub(crate) async fn run_filesystem_ls(
    invocation: &Invocation,
    args: FilesystemLsArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = NamespaceContext::resolve(invocation, None).await?;
    let root = RootPath::Allowed;
    let spec = namespace_path(
        context.namespace(),
        "path",
        args.path.as_deref().unwrap_or("/"),
        root,
    )
    .map_err(|error| context.fail(error))?;
    let at = ReadAt::from(args.snapshot_id.clone());
    let output = invocation.behavior.output;
    let streams_pages = args.pagination.page_limits.jsonl
        || (args.pagination.page_limits.all && output == OutputFormat::Human);
    if streams_pages {
        let followed = follow_path_entry_pages(&context, &spec, &args.pagination, &at, |entries| {
            crate::render::write_path_entries_page(&entries, args.pagination.page_limits.jsonl)
                .map_err(CliError::io)
        })
        .await?;
        // Write head-drift warnings to stderr so streamed stdout contains
        // only entries.
        if let Some(drift) = followed.head_drift {
            crate::render::write_listing_drift_warning(&drift);
        }
        return Ok(context.output(CommandData::StreamedToStdout));
    }

    let mut entries = Vec::new();
    let followed = follow_path_entry_pages(&context, &spec, &args.pagination, &at, |page| {
        entries.extend(page);
        Ok(())
    })
    .await?;
    if output == OutputFormat::Human {
        if let Some(drift) = followed.head_drift.as_ref() {
            crate::render::write_listing_drift_warning(drift);
        }
    }
    Ok(context.output(CommandData::PathEntries {
        namespace_id: followed.namespace_id,
        path: followed.path,
        head_seq: followed.head_seq,
        head_drift: followed.head_drift,
        entries,
        next_cursor: followed.next_cursor,
    }))
}

pub(crate) async fn run_filesystem_stat(
    invocation: &Invocation,
    args: FilesystemStatArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = NamespaceContext::resolve(invocation, None).await?;
    let options = StatOptions {
        at: ReadAt::from(args.snapshot_id.clone()),
        ..StatOptions::default()
    };
    let client = &context.profile.target.client;
    let entry = match args.inode {
        Some(inode_id) => {
            client
                .stat_by_inode_with_options(context.namespace(), inode_id, &options)
                .await
        }
        None => {
            let path = args
                .path
                .as_deref()
                .expect("clap should require either a path or `--inode`");
            let root = RootPath::Allowed;
            let spec = namespace_path(context.namespace(), "path", path, root)
                .map_err(|error| context.fail(error))?;
            client.stat_with_options(&spec, &options).await
        }
    }
    .map_err(|error| context.fail(error))?;

    Ok(context.output(CommandData::PathEntry(entry)))
}

/// One `--set key=value` argument. The key ends at the first `=` so a value
/// may contain more of them, and a missing `=` names the flag rather than
/// guessing what the caller meant.
fn parse_attribute_assignment(argument: &str) -> Result<(AttributeKey, AttributeValue)> {
    let Some((key, value)) = argument.split_once('=') else {
        return Err(CliError::invalid_request(format!(
            "invalid `--set` {argument:?}: expected `key=value`"
        ))
        .with_param("--set"));
    };
    Ok((
        parse_attribute_key_arg("--set", key)?,
        AttributeValue::parse(value).map_err(|error| {
            CliError::invalid_request(format!("invalid `--set` value: {error}")).with_param("--set")
        })?,
    ))
}

fn parse_attribute_key_arg(flag: &str, key: &str) -> Result<AttributeKey> {
    AttributeKey::parse(key).map_err(|error| {
        CliError::invalid_request(format!("invalid `{flag}` key: {error}")).with_param(flag)
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

fn attribute_update(
    args: &FilesystemAnnotateArgs,
) -> Result<(AttributeChanges, UpdateAttributesOptions)> {
    let (set, remove) = match args.attributes_json.as_deref() {
        Some(document) => {
            let update: AttributeUpdateJson = serde_json::from_str(document).map_err(|error| {
                CliError::invalid_request(format!(
                    "invalid `--attributes-json` attribute update: {error}"
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
    let options = UpdateAttributesOptions {
        commit: commit_options(&args.commit),
        expected_inode_id: args.expected_inode_id,
        expected_attributes_revision_no: args.expected_attributes_revision,
    };
    Ok((AttributeChanges { set, remove }, options))
}

pub(crate) async fn run_filesystem_annotate(
    invocation: &Invocation,
    args: FilesystemAnnotateArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = NamespaceContext::resolve(invocation, args.actor.actor_id.as_ref()).await?;
    let root = RootPath::Allowed;
    let spec = namespace_path(context.namespace(), "path", &args.path, root)
        .map_err(|error| context.fail(error))?;
    let (changes, options) = attribute_update(&args).map_err(|error| context.fail(error))?;
    let result = context
        .profile
        .target
        .client
        .update_attributes_with_options(&spec, context.actor(), changes, &options)
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

pub(crate) async fn run_filesystem_grep(
    invocation: &Invocation,
    args: FilesystemGrepArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = NamespaceContext::resolve(invocation, None).await?;
    let path_prefix = args
        .path_prefix
        .as_deref()
        .map(|path| parse_user_path_arg("--path-prefix", path, RootPath::Allowed))
        .transpose()
        .map_err(|error| context.fail(error))?;
    let request = loonfs_types::GrepRequest {
        pattern: args.pattern.clone(),
        case_insensitive: args.ignore_case,
        scope: path_prefix.map_or(
            loonfs_types::GrepScope::Namespace,
            loonfs_types::GrepScope::PathPrefix,
        ),
        allow_stale: args.allow_stale,
        allow_scan: args.allow_scan,
    };
    let mut tail_scanned = true;
    let listing = collect_or_stream_pages(
        context
            .profile
            .target
            .client
            .grep(context.namespace(), &request)
            .cursor(args.pagination.cursor.clone()),
        &args.pagination.page_limits,
        |page: &GrepResponse| tail_scanned &= page.tail_scanned,
    )
    .await
    .map_err(|error| context.fail(grep_index_hint(error)))?;
    let PagedListing::Collected(response) = listing else {
        if !tail_scanned {
            crate::render::write_stderr_warning(
                "recent commits were not scanned (allow_stale); results may be stale",
            );
        }
        return Ok(context.output(CommandData::StreamedToStdout));
    };
    Ok(context.output(CommandData::GrepMatches {
        pattern: args.pattern,
        namespace_id: response.namespace_id,
        head_seq: response.head_seq,
        built_through_seq: response.built_through_seq,
        matches: response.matches,
        tail_scanned: response.tail_scanned,
        truncated: response.next_cursor.is_some(),
        next_cursor: response.next_cursor,
    }))
}

/// Adds how to enable the grep index to the error of a grep against a
/// namespace without an active index.
fn grep_index_hint(mut error: CliError) -> CliError {
    if ErrorCode::parse(&error.code) == Some(ErrorCode::NotSupported)
        && error.feature.as_deref() == Some("query.grep")
    {
        error
            .message
            .push_str("; enable it with `loonfs maintenance index enable`");
    }
    error
}

pub(crate) async fn run_filesystem_cat(
    invocation: &Invocation,
    args: FilesystemCatArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = NamespaceContext::resolve(invocation, None).await?;
    let root = RootPath::Refused;
    let spec = namespace_path(context.namespace(), "path", &args.path, root)
        .map_err(|error| context.fail(error))?;
    let version = FileVersion::from_flags(args.revision, args.snapshot_id);
    stream_file_to_stdout(&context, &spec, &version)
        .await
        .map_err(|error| context.fail(error))?;
    Ok(context.output(CommandData::StreamedToStdout))
}

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
    let snapshot_id = args.snapshot_id.clone();
    let entry = context
        .profile
        .target
        .stat_without_attributes_at_snapshot(&spec, snapshot_id.as_ref())
        .await
        .map_err(|error| context.fail(error))?;
    if args.recursive {
        if entry.inode_kind() != EntryInodeKind::Directory {
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
        return recursive::run_get_tree(
            &context,
            spec.absolute_path(),
            &local_root,
            destination_behavior(args.force),
            behavior,
            &ReadAt::from(snapshot_id),
        )
        .await;
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

    let version = FileVersion::from_flags(args.revision, snapshot_id);
    if args.local_destination.as_deref() == Some("-") {
        // No progress and no resume: standard output is carrying the file,
        // and bytes already piped onward are somewhere this CLI cannot see.
        stream_file_to_stdout(&context, &spec, &version)
            .await
            .map_err(|error| context.fail(error))?;
        return Ok(context.output(CommandData::StreamedToStdout));
    }

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
    } = open_resumable_download(&context, &spec, &version, &destination)
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
pub(super) struct ResumableDownload {
    pub(super) download: FileDownload,
    pub(super) partial: Option<PartialDownloadMetadata>,
}

/// Opens a download at the offset recorded in a matching partial file.
pub(super) async fn open_resumable_download(
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
pub(super) async fn stream_download_to_file(
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
pub(super) enum DestinationName {
    /// The caller named the destination.
    Given,
    /// The name is the remote file's name.
    Derived,
}

/// The destination behavior `--force` asks for.
fn destination_behavior(force: bool) -> DestinationBehavior {
    if force {
        DestinationBehavior::Replace
    } else {
        DestinationBehavior::NoReplace
    }
}

/// Downloads one file and writes it to standard output as it arrives.
///
/// Bytes are handed on chunk by chunk, so a streamed download that fails its
/// verification at the end fails after some of the file has already been
/// written. That is what streaming to a pipe means — `cat` behaves the same
/// way — and it is why the exit status, not the output, is what says whether
/// the content was verified.
async fn stream_file_to_stdout(
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

/// A local file that a download writes, and the facts that shape a failure
/// to write it.
#[derive(Debug, Clone, Copy)]
pub(super) struct LocalDestination<'a> {
    pub(super) path: &'a Path,
    pub(super) behavior: DestinationBehavior,
    pub(super) name: DestinationName,
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

pub(crate) async fn run_filesystem_trash(
    invocation: &Invocation,
    args: TrashArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = NamespaceContext::resolve(invocation, None).await?;
    let listing = collect_or_stream_pages(
        context
            .profile
            .target
            .client
            .list_trash(context.namespace())
            .cursor(args.pagination.cursor.clone()),
        &args.pagination.page_limits,
        |_: &loonfs_types::ListTrashResponse| {},
    )
    .await
    .map_err(|error| context.fail(error))?;
    let PagedListing::Collected(response) = listing else {
        return Ok(context.output(CommandData::StreamedToStdout));
    };
    let hint = UndeleteHint::new(
        &context,
        &invocation.config,
        ProfileSource::from_selectors(&invocation.selectors),
    );
    let recovery_commands = response
        .entries
        .iter()
        .map(|entry| hint.command(entry.inode_id, entry.deletion_seq))
        .collect();
    Ok(context.output(CommandData::Trash(TrashListing {
        response,
        recovery_commands,
    })))
}

pub(crate) async fn run_filesystem_revisions(
    invocation: &Invocation,
    args: FilesystemRevisionsArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = NamespaceContext::resolve(invocation, None).await?;
    let root = RootPath::Refused;
    let spec = namespace_path(context.namespace(), "path", &args.path, root)
        .map_err(|error| context.fail(error))?;
    let listing = collect_or_stream_pages(
        context
            .profile
            .target
            .client
            .list_file_revisions(&spec)
            .cursor(args.pagination.cursor.clone()),
        &args.pagination.page_limits,
        |_: &loonfs_types::ListFileRevisionsResponse| {},
    )
    .await
    .map_err(|error| context.fail(error))?;
    let PagedListing::Collected(response) = listing else {
        return Ok(context.output(CommandData::StreamedToStdout));
    };

    Ok(context.output(CommandData::FileRevisions {
        target: render_target(context.namespace(), spec.absolute_path()),
        revisions: response.revisions,
        next_cursor: response.next_cursor,
    }))
}

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
    let local_leaf =
        super::context::utf8_local_name(local_leaf).map_err(|error| context.fail(error))?;
    let remote_path = match args.remote_path.as_deref() {
        // A trailing slash names the directory the file lands in — the
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
/// it is always read once and never held.
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
pub(super) async fn put_payload(
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

/// Commits an upload whose bytes all landed before it was interrupted,
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

/// Reads the bytes one append carries. The read stops one byte past what an
/// append may carry, so an oversized source never fills memory and the
/// server still answers it with `content_too_large`.
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
    // handle `loonfs undelete` needs. The delete then carries it as an
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
    let recovery_command = UndeleteHint::new(
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
    let root = RootPath::Refused;
    let spec = namespace_path(context.namespace(), "path", &args.path, root)
        .map_err(|error| context.fail(error))?;
    let commit = commit_options(&args.commit);
    let revision_no = args.revision;
    let result = context
        .profile
        .target
        .client
        .restore_revision_with_options(&spec, revision_no, context.actor(), &commit)
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

pub(crate) async fn run_filesystem_undelete(
    invocation: &Invocation,
    args: FilesystemUndeleteArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = NamespaceContext::resolve(invocation, args.actor.actor_id.as_ref()).await?;
    let root = RootPath::Refused;
    // An absent path restores in place; the destination is then the parent
    // and name the deletion recorded, which no path here could name better.
    let spec = args
        .destination_path
        .as_deref()
        .map(|path| namespace_path(context.namespace(), "destination_path", path, root))
        .transpose()
        .map_err(|error| context.fail(error))?;
    let commit = commit_options(&args.commit);
    let deletion_seq = args.deletion_seq;
    let destination = spec.as_ref().map_or(UndeleteDestination::Recorded, |spec| {
        UndeleteDestination::Path(spec.absolute_path().clone())
    });
    let result = context
        .profile
        .target
        .client
        .undelete_with_options(
            context.namespace(),
            args.inode,
            deletion_seq,
            context.actor(),
            &UndeleteOptions {
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
        inode_id: Some(args.inode),
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
    context: &NamespaceContext,
    named: NamespacePath,
    source_leaf: &str,
) -> Result<NamespacePath> {
    let Ok(existing) = context.profile.target.stat_without_attributes(&named).await else {
        // Absent, or unreadable for a reason the transfer itself will
        // report: either way this is not a directory to land inside.
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
    // directory to land in, and the leaf is already appended; looking again
    // would append it twice.
    let to = if directory_intent(&args.destination_path) || args.destination_path == "/" {
        named_destination
    } else {
        resolve_transfer_destination(&context, named_destination, &source_leaf)
            .await
            .map_err(|error| context.fail(error))?
    };

    let commit = commit_options(&args.commit);
    let expected_destination_revision_no = args.expected_destination_revision;
    let behavior = if args.force
        || args.expected_destination_inode_id.is_some()
        || expected_destination_revision_no.is_some()
    {
        DestinationBehavior::Replace
    } else {
        DestinationBehavior::NoReplace
    };
    let result = if transfer_kind == TransferKind::Copy {
        let entry = context
            .profile
            .target
            .stat_without_attributes(&from)
            .await
            .map_err(|error| context.fail(error))?;
        if args.recursive {
            if entry.inode_kind() != EntryInodeKind::Directory {
                return Err(context.fail(CliError::invalid_request(format!(
                        "`{}` is not a directory; drop -r to copy one file",
                        from.absolute_path()
                    ))
                    .with_param("-r"),
                ));
            }
            if args.commit.commit_id.is_some() {
                return Err(context.fail(CliError::invalid_request(
                        "`--commit-id` names one commit; a recursive copy makes one commit per item",
                    )
                    .with_param("--commit-id"),
                ));
            }
            return recursive::run_copy_tree(
                &context,
                from.absolute_path(),
                to.absolute_path(),
                destination_behavior(args.force),
                args.commit.message.clone(),
                invocation.behavior,
            )
            .await;
        }
        if entry.inode_kind() == EntryInodeKind::Directory {
            return Err(context.fail(CliError::invalid_request(format!(
                    "`{}` is a directory; use `loonfs cp -r` to copy the tree",
                    from.absolute_path()
                ))
                .with_param("source_path"),
            ));
        }
        context
            .profile
            .target
            .client
            .copy_path_with_options(
                &from,
                &to,
                context.actor(),
                &loonfs_client::CopyOptions {
                    behavior,
                    commit,
                    expected_destination_inode_id: args.expected_destination_inode_id,
                    expected_destination_revision_no,
                },
            )
            .await
    } else {
        context
            .profile
            .target
            .client
            .move_path_with_options(
                &from,
                &to,
                context.actor(),
                &loonfs_client::MoveOptions {
                    behavior,
                    commit,
                    expected_destination_inode_id: args.expected_destination_inode_id,
                    expected_destination_revision_no,
                },
            )
            .await
    }
    .map_err(|error| context.fail(error))?;

    Ok(context.output(CommandData::PathMove {
        from: render_target(context.namespace(), from.absolute_path()),
        to: render_target(context.namespace(), to.absolute_path()),
        committed_seq: result.committed_seq,
        commit_id: result.commit_id,
    }))
}
