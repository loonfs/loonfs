//! `loonfs namespace` commands: create, show, fork, and delete.

use super::context::{Invocation, ProfileContext};
use super::output::{CommandData, CommandFailure, CommandOutput};
use crate::args::{
    NamespaceAccessArg, NamespaceCommand, NamespaceCreateArgs, NamespaceDeleteArgs,
    NamespaceForkArgs, NamespaceShowArgs, NamespaceUseArgs,
};
use crate::config::{load_config, mutate_config};
use crate::error::{CliError, Result};
use crate::profiles::{resolve_profile, set_default_namespace, NamedProfile};
use crate::prompt::prompt_line;
use crate::resolve::resolve_namespace;
use loonfs_client::{CreateNamespaceOptions, DeleteNamespaceOptions, ForkNamespaceOptions};
use loonfs_types::{AccessGrants, AccessRights, NamespaceAccess, PrincipalScope};
use std::collections::BTreeMap;

pub(crate) async fn run_namespace_command(
    invocation: &Invocation,
    command: NamespaceCommand,
) -> Result<CommandOutput, CommandFailure> {
    match command {
        NamespaceCommand::Create(args) => run_namespace_create(invocation, args).await,
        NamespaceCommand::Show(args) => run_namespace_show(invocation, args).await,
        NamespaceCommand::Delete(args) => run_namespace_delete(invocation, args).await,
        NamespaceCommand::Fork(args) => run_namespace_fork(invocation, args).await,
    }
}

async fn run_namespace_show(
    invocation: &Invocation,
    args: NamespaceShowArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = ProfileContext::resolve_as_subject(invocation).await?;
    let explicit_namespace = args
        .namespace_id
        .as_ref()
        .or(invocation.selectors.namespace.as_ref());
    let namespace_id = context.namespace(explicit_namespace)?;
    let namespace = context
        .target
        .client
        .get_namespace(&namespace_id)
        .await
        .map_err(|error| context.fail(error))?;

    Ok(context.output(CommandData::NamespaceStatus(namespace)))
}

async fn run_namespace_create(
    invocation: &Invocation,
    args: NamespaceCreateArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = ProfileContext::resolve(invocation).await?;
    let actor_id = context.actor(args.actor.actor_id.as_ref())?;
    let access = namespace_access(invocation, &args).map_err(|error| context.fail(error))?;
    let namespace = context
        .target
        .client
        .create_namespace_with_options(
            &args.namespace_id,
            &actor_id,
            &CreateNamespaceOptions {
                access,
                retention_window_ms: args.retention_window_ms,
                allow_existing: false,
            },
        )
        .await
        .map_err(|error| context.fail(error))?;

    Ok(context.output(CommandData::NamespaceStatus(namespace)))
}

fn namespace_access(
    invocation: &Invocation,
    args: &NamespaceCreateArgs,
) -> Result<NamespaceAccess> {
    match args.access {
        NamespaceAccessArg::Unrestricted => Ok(NamespaceAccess::unrestricted()),
        NamespaceAccessArg::Acl => {
            let principal_scope = invocation
                .selectors
                .subject
                .principal_scope
                .as_deref()
                .ok_or_else(|| {
                    CliError::invalid_request("`--principal-scope` is required with `--access acl`")
                        .with_param("--principal-scope")
                })?;
            let principal_scope = PrincipalScope::parse(principal_scope).map_err(|error| {
                CliError::invalid_request(error.to_string()).with_param("--principal-scope")
            })?;
            let root_grants = args
                .administrators
                .iter()
                .map(|principal| (principal.clone(), AccessRights::ADMIN))
                .collect::<BTreeMap<_, _>>();
            let root_grants = AccessGrants::new(root_grants).map_err(|error| {
                CliError::invalid_request(error.to_string()).with_param("--administrator")
            })?;
            Ok(NamespaceAccess::Acl {
                principal_scope,
                root_grants,
            })
        }
    }
}

async fn run_namespace_delete(
    invocation: &Invocation,
    args: NamespaceDeleteArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = ProfileContext::resolve_as_subject(invocation).await?;

    if !args.yes {
        // Without a terminal (or under --no-input / --json) there is no
        // prompt to answer; say what is required instead of surfacing the
        // prompt machinery's i/o error.
        if !invocation.behavior.interactive {
            return Err(context.fail(CliError::non_interactive_input_required(
                "deleting a namespace requires confirmation: pass `--yes`, or run \
                 interactively to confirm at the prompt",
            )));
        }
        // Deletion is terminal and retires the id; require the operator to
        // type the namespace id back (or pass --yes).
        let typed = prompt_line(&format!(
            "deleting `{}` is permanent and retires the id; type the namespace id to confirm",
            args.namespace_id
        ))
        .map_err(|error| context.fail(error))?;
        if typed.trim() != args.namespace_id.as_str() {
            return Err(context.fail(CliError::invalid_request(format!(
                "confirmation `{typed}` does not match namespace id `{}`",
                args.namespace_id
            ))));
        }
    }

    let response = context
        .target
        .client
        .delete_namespace_with_options(
            &args.namespace_id,
            &DeleteNamespaceOptions {
                expected_head_seq: args.expected_head_seq,
            },
        )
        .await
        .map_err(|error| context.fail(error))?;

    Ok(context.output(CommandData::NamespaceDeleted(response)))
}

async fn run_namespace_fork(
    invocation: &Invocation,
    args: NamespaceForkArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = ProfileContext::resolve_as_subject(invocation).await?;
    let actor_id = context.actor(args.actor.actor_id.as_ref())?;
    let namespace = context
        .target
        .client
        .fork_namespace_with_options(
            &args.source_namespace_id,
            &args.new_namespace_id,
            &actor_id,
            &ForkNamespaceOptions {
                snapshot_id: args.snapshot_id,
            },
        )
        .await
        .map_err(|error| context.fail(error))?;

    Ok(context.output(CommandData::NamespaceStatus(namespace)))
}

pub(crate) async fn run_namespace_use(
    invocation: &Invocation,
    args: NamespaceUseArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = ProfileContext::resolve(invocation).await?;

    context
        .target
        .client
        .get_namespace(&args.namespace_id)
        .await
        .map_err(|error| context.fail(error))?;

    mutate_config(&invocation.config.path, |config| {
        set_default_namespace(config, &context.profile_name, args.namespace_id.as_str())
    })
    .map_err(|error| context.fail(error))?;

    Ok(context.output(CommandData::DefaultNamespace {
        profile: context.profile_name.clone(),
        namespace: args.namespace_id.to_string(),
    }))
}

pub(crate) fn run_namespace_current(
    invocation: &Invocation,
) -> Result<CommandOutput, CommandFailure> {
    let explicit_profile = invocation.selectors.profile.as_deref();
    let config = load_config(&invocation.config.path)
        .map_err(|error| invocation.fail_for_profile(explicit_profile, error))?;
    let NamedProfile {
        name: profile_name,
        profile,
    } = resolve_profile(&config, explicit_profile)
        .map_err(|error| invocation.fail_for_profile(explicit_profile, error))?;
    let mode = profile.mode();
    // `current` is a status command, so an unset namespace is returned as
    // `null`. Environment selection still takes precedence over the profile
    // default.
    let namespace = match resolve_namespace(&profile_name, profile, None) {
        Ok(namespace) => Some(namespace.to_string()),
        Err(error) if error.is_no_default_namespace() => None,
        Err(error) => {
            return Err(super::context::fail(
                invocation.kind,
                Some(profile_name),
                Some(mode),
                error,
            ))
        }
    };

    Ok(CommandOutput {
        kind: invocation.kind,
        profile: Some(profile_name.clone()),
        mode: Some(mode),
        data: CommandData::Current {
            profile: profile_name,
            namespace,
        },
    })
}
