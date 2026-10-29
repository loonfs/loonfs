//! The `ls`, `stat`, `trash`, and `revisions` commands.

use crate::args::{
    FilesystemLsArgs, FilesystemRevisionsArgs, FilesystemStatArgs, PaginationArgs, TrashArgs,
};
use crate::commands::context::{
    namespace_path, render_target, Invocation, NamespaceContext, ProfileSource, RootPath,
    UndeleteHint,
};
use crate::commands::output::{
    CommandData, CommandFailure, CommandOutput, ListingHeadDrift, ListingHeadObservation,
    TrashListing,
};
use crate::commands::pagination::{collect_or_stream_pages, visit_pages, PagedListing};
use crate::error::{CliError, Result};
use crate::render::OutputFormat;
use loonfs_client::{ListOptions, NamespacePath, ReadAt, StatOptions};
use loonfs_types::{AbsolutePath, ChangeSeq, ListPathEntriesResponse, NamespaceId};

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
