//! Replaces access rows on namespace paths.

use super::context::{namespace_path, render_target, resolve_mutation_context};
use super::fs::commit_options;
use super::output::{CommandData, CommandFailure, CommandOutput};
use crate::args::{AccessSetArgs, CommandKind};
use crate::error::CliError;
use loonfs_api::{AccessGrants, AccessRevisionNo, AccessRight, AccessRights, PrincipalId};
use loonfs_client::UpdateAccessOptions;
use std::collections::BTreeMap;
use std::path::Path;

pub(crate) async fn run_access_set(
    kind: CommandKind,
    config_path: &Path,
    args: AccessSetArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_mutation_context(kind, config_path, &args.target, &args.actor).await?;
    let spec = namespace_path(context.namespace(), "path", &args.path, true)
        .map_err(|error| context.fail(kind, error))?;
    let options = UpdateAccessOptions {
        boundary: args.boundary,
        grants: parse_grants(&args.grants).map_err(|error| context.fail(kind, error))?,
        commit: commit_options(context.actor(), &args.commit)
            .map_err(|error| context.fail(kind, error))?,
        expected_inode_id: args.expected_inode_id,
        expected_access_revision_no: args.expected_revision.map(AccessRevisionNo),
    };
    let result = context
        .target
        .client
        .update_access(&spec, &options)
        .await
        .map_err(|error| context.fail(kind, error))?;
    Ok(context.output(
        kind,
        CommandData::FileMutation {
            target: render_target(context.namespace(), spec.absolute_path()),
            committed_seq: result.committed_seq,
            commit_id: result.commit_id,
            inode_id: None,
            recovery_command: None,
        },
    ))
}

fn parse_grants(grants: &[String]) -> Result<AccessGrants, CliError> {
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
        let mut rights = AccessRights::EMPTY;
        for name in names.split(',') {
            let right = AccessRight::ALL
                .iter()
                .find(|right| right.as_str() == name)
                .ok_or_else(|| invalid_grant(format!("unknown access right {name:?}")))?;
            rights.insert(*right);
        }
        if entries.insert(principal, rights).is_some() {
            return Err(invalid_grant(format!(
                "repeated principal in grant {grant:?}"
            )));
        }
    }
    AccessGrants::new(entries).map_err(|error| invalid_grant(error.to_string()))
}
