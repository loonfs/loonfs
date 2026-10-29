//! Replaces access rows on namespace paths.

use super::context::{namespace_path, render_target, Invocation, NamespaceContext, RootPath};
use super::fs::commit_options;
use super::output::{CommandData, CommandFailure, CommandOutput};
use crate::args::AccessSetArgs;
use crate::error::{CliError, Result};
use loonfs_client::{AccessState, UpdateAccessOptions};
use loonfs_types::{AccessGrants, AccessRight, AccessRights, PrincipalId};
use std::collections::BTreeMap;

pub(crate) async fn run_access_set(
    invocation: &Invocation,
    args: AccessSetArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = NamespaceContext::resolve(invocation, args.actor.actor_id.as_ref()).await?;
    let spec = namespace_path(context.namespace(), "path", &args.path, RootPath::Allowed)
        .map_err(|error| context.fail(error))?;
    let access = AccessState {
        boundary: args.boundary,
        grants: parse_grants(&args.grants).map_err(|error| context.fail(error))?,
    };
    let options = UpdateAccessOptions {
        commit: commit_options(&args.commit),
        expected_inode_id: args.expected_inode_id,
        expected_access_revision_no: args.expected_revision,
    };
    let result = context
        .profile
        .target
        .client
        .update_access_with_options(&spec, context.actor(), access, &options)
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

fn parse_grants(grants: &[String]) -> Result<AccessGrants> {
    let invalid_grant = |message| CliError::invalid_request(message).with_param("--grant");
    let mut entries = BTreeMap::new();
    for grant in grants {
        let (principal, names) = grant.split_once('=').ok_or_else(|| {
            invalid_grant(format!(
                "invalid grant {grant:?}: expected principal=right[,right...]"
            ))
        })?;
        let principal =
            PrincipalId::parse(principal).map_err(|error| invalid_grant(error.to_string()))?;
        let rights = names
            .split(',')
            .map(str::parse::<AccessRight>)
            .collect::<Result<Vec<_>, _>>()
            .and_then(AccessRights::from_distinct)
            .map_err(|error| invalid_grant(error.to_string()))?;
        if entries.insert(principal, rights).is_some() {
            return Err(invalid_grant(format!(
                "repeated principal in grant {grant:?}"
            )));
        }
    }
    AccessGrants::new(entries).map_err(|error| invalid_grant(error.to_string()))
}
