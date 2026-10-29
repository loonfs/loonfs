//! The `annotate`, `grep`, and `cat` commands.

use super::get::stream_file_to_stdout;
use super::mutate::commit_options;
use crate::args::{FilesystemAnnotateArgs, FilesystemCatArgs, FilesystemGrepArgs};
use crate::commands::context::{
    namespace_path, parse_user_path_arg, render_target, Invocation, NamespaceContext, RootPath,
};
use crate::commands::download::FileVersion;
use crate::commands::output::{CommandData, CommandFailure, CommandOutput};
use crate::commands::pagination::{collect_or_stream_pages, PagedListing};
use crate::error::{CliError, Result};
use loonfs_client::{AttributeChanges, UpdateAttributesOptions};
use loonfs_types::{AttributeKey, AttributeValue, ErrorCode, GrepResponse};
use std::collections::BTreeMap;

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

/// The `--attributes-json` form of the update: one object with the same
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
